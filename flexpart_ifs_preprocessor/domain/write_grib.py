
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict

import xarray as xr

from flexprep.input_fields import CONSTANT_FIELDS
from flexprep.io_grib import _meta_dict, _pick_grib2_template_metadata, _override_time, _unset_surface_scaled_values, _apply_vertical_overrides, _to_minutes, _get_valid_time


logger = logging.getLogger(__name__)

# Short names written as 1-hour accumulations (PDT 8)
ACCUM_SHORTNAMES: set[str] = {"lsp", "sshf", "ewss", "nsss", "cp", "ssr"}


def write_grib(
    fields: dict[str, xr.DataArray],
    output_dir: str | Path = "./",
    *,
    prefix: str = "dispf",
    suffix: str = ".grib",
) -> list[Path]:
    """
    Write one GRIB file per forecast step by overriding message metadata.

    Parameters
    ----------
    fields
        Mapping {shortName: DataArray}. Non-constant fields must have a 'step'
        coordinate (timedelta-like) and carry GRIB metadata at
        DataArray.earthkit.metadata.
    output_dir
        Directory to write files (created if missing).
    prefix
        Prefix to include in the filename.
        The filename will be constructed as "{prefix}{valid_time:YYYYMMDDHH}{suffix}".
        Default: "dispf".
        Use "" to omit.
    suffix
        File extension/suffix
        Default: ".grib"
        Use "" to omit.

    Returns
    -------
    list[pathlib.Path]
        Paths written (one per step).
    """
    if not fields:
        raise ValueError("fields is empty.")

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Baseline: GRIB2, PDT 4.0 message used as template for overrides.
    ref_md: Any = _pick_grib2_template_metadata(fields)

    # Steps from a representative field
    rep = fields["u"] if "u" in fields else next(iter(fields.values()))
    if "step" not in rep.coords:
        raise ValueError("Expected a 'step' coordinate on at least one field.")
    steps = rep.coords["step"].values

    written: list[Path] = []

    for step in steps:
        # Build per-step dict
        fields_step: dict[str, xr.DataArray] = {}
        for name, da in fields.items():
            if name in CONSTANT_FIELDS:
                fields_step[name] = da
            else:
                if "step" not in da.coords:
                    raise ValueError(f"Field '{name}' is missing 'step' coord.")
                fields_step[name] = da.sel(step=step)

        valid_time = _get_valid_time(fields_step, step)
        out_path = out_dir / f"{prefix}{valid_time}{suffix}"
        logger.info("Writing GRIB: %s", out_path)

        # Create/empty file
        with open(out_path, "wb"):
            pass

        step_min = _to_minutes(step)
        step_h = int(step_min // 60)

        for name, field in fields_step.items():
            # Skip empty
            if field.isnull().all():
                logger.info("Ignoring '%s' - only NaN values.", name)
                continue

            meta: Dict[str, Any] = _meta_dict(field)
            shortname = (meta.get("shortName") or name)

            # Start from template & set shortName
            md: Any = ref_md.override(shortName=shortname)

            # Determine if this message is an hourly accumulation
            window_h = 1 if shortname in ACCUM_SHORTNAMES else 0
            md = _override_time(md, step_h=step_h, window_h=window_h, short_name=shortname)

            # ---------------- processed accumulations (PDT 8) ----------------
            if shortname in ACCUM_SHORTNAMES:
                # Keep only shortName + surface level (no triple), then unset scale keys
                md = md.override(
                    shortName=shortname,
                    typeOfFirstFixedSurface=1,
                    typeOfSecondFixedSurface=255,
                )
                md = _unset_surface_scaled_values(md)

            # ---------------- omega / w (instantaneous, hybrid level K) ------
            elif name in {"omega", "w"} or shortname in {"omega", "w"}:
                lvl = int(meta.get("level", 1))
                md = md.override(
                    productDefinitionTemplateNumber=0,   # instantaneous
                    indicatorOfUnitOfTimeRange=1,        # hours
                    stepUnits=1,
                    forecastTime=step_h,
                    stepRange=str(step_h),

                    typeOfFirstFixedSurface=105,         # hybrid level
                    scaleFactorOfFirstFixedSurface=0,
                    scaledValueOfFirstFixedSurface=lvl,
                    typeOfSecondFixedSurface=255,

                    shortName="etadot",
                )

            # ------------- unprocessed, GRIB1→GRIB2 vertical fixes ----------
            else:
                md = _apply_vertical_overrides(md, shortname)

            # Inject raw message buffer and append
            # md._handle is an ecCodes handle (opaque to typing)
            field.attrs["_earthkit"] = {"message": md._handle.get_buffer()}  # type: ignore[attr-defined]

            with tempfile.NamedTemporaryFile(suffix="") as tmp:
                field.earthkit.to_grib(tmp.name)  # type: ignore[attr-defined]
                with open(tmp.name, "rb") as fh_in, open(out_path, "ab") as fh_out:
                    fh_out.write(fh_in.read())

        written.append(out_path)
        logger.info("Saved: %s", out_path)

    return written
