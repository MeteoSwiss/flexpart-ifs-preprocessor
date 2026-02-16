import typing
from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass
class IFSForecast:
    row_id: int
    forecast_ref_time: datetime
    step: int
    key: str
    processed: bool

    def to_dict(self) -> dict[str, typing.Any]:
        return asdict(self)
