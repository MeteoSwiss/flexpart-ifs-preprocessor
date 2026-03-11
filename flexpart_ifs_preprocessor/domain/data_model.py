import typing
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import re
from enum import Enum


logger = logging.getLogger(__name__)

class Domain(Enum):
    GLOBAL = "global"
    EUROPE = "europe"

@dataclass
class IFSForecastFile:
    forecast_ref_time: datetime
    step: int
    object_key: str
    filename: str
    processed: bool
    domain: Domain = field(init=False)

    def to_dict(self) -> dict[str, typing.Any]:
        return asdict(self)

    def __post_init__(self) -> None:
        if "_F1_" in self.filename.upper():
            self.domain = Domain.EUROPE
        elif "_F2_" in self.filename.upper():
            self.domain = Domain.GLOBAL
        else:
            logger.error("Unknown domain for file %s: %s", self.filename)
            raise ValueError(f"Unknown domain for file {self.filename}")
@dataclass
class InputDataAggregatorEvent:
    object_key: str
    filename: str
    forecast_ref_time: datetime
    step: int

    def __init__(self, data: dict) -> None:
        self.object_key = data['objectStoreUuid']
        self.filename = data['fileName']

        self.forecast_ref_time = self._extract_datetime(self.filename)
        self.step = self.extract_lead_time(self.filename)

    def _extract_datetime(self, s: str) -> datetime:
        match = re.search(r'(\d{8}T\d{6}Z)', s)
        if not match:
            raise ValueError(f"No datetime found in string: {s}")
        return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)

    def extract_lead_time(self, s: str) -> int:
        match = re.search(r'_(\d+)([a-z]+)$', s)
        if not match:
            raise ValueError(f"No lead time found in string: {s}")
        value, unit = match.group(1), match.group(2)
        assert unit == "h", f"Expected unit 'h', got '{unit}'"
        return int(value)
