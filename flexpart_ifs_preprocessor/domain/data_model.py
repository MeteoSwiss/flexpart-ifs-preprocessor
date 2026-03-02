import typing
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import re

@dataclass
class IFSForecastFile:
    forecast_ref_time: datetime
    step: int
    object_key: str
    filename: str
    processed: bool

    def to_dict(self) -> dict[str, typing.Any]:
        return asdict(self)

@dataclass
class InputDataAggregatorEvent:
    object_key: str
    filename: str
    forecast_ref_time: datetime
    step: str

    def __init__(self, data):
        self.object_key = data['objectStoreUuid']
        self.filename = data['fileName']

        self.forecast_ref_time = self._extract_datetime(self.filename)
        self.step = self.extract_lead_time(self.filename)

    def _extract_datetime(s: str) -> datetime:
        match = re.search(r'(\d{8}T\d{6}Z)', s)
        if not match:
            raise ValueError(f"No datetime found in string: {s}")
        return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)

    def extract_lead_time(s: str) -> int:
        match = re.search(r'_(\d+)([a-z]+)$', s)
        if not match:
            raise ValueError(f"No lead time found in string: {s}")
        value, unit = match.group(1), match.group(2)
        assert unit == "h", f"Expected unit 'h', got '{unit}'"
        return int(value)
