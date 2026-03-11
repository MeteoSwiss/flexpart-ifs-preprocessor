import typing
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import re
from enum import Enum


logger = logging.getLogger(__name__)

class Stream(Enum):
    S4Y = "S4Y"
    S5Y = "S5Y"
    S6Y = "S6Y"
    UNKNOWN = "UNKNOWN"

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

@dataclass
class InputDataAggregatorEvent:
    object_key: str
    filename: str
    forecast_ref_time: datetime = field(init=False)
    step: int = field(init=False)
    domain: Domain = field(init=False)
    stream: Stream = field(init=False)

    def __post_init__(self, data: dict) -> None:
        self.object_key = data['objectStoreUuid']
        self.filename = data['fileName']

        self.forecast_ref_time = self._extract_datetime(self.filename)
        self.step = self.extract_lead_time(self.filename)
        self.stream = self._extract_stream()
        self.domain = self._extract_domain()


    def _extract_stream(self) -> Stream:
        match = re.search(r'(S[456]Y)', self.filename.upper())
        if not match:
            return Stream.UNKNOWN
        return Stream(match.group(1))

    def _extract_domain(self) -> Domain:
        if "_F1_" in self.filename.upper():
            return Domain.EUROPE
        elif "_F2_" in self.filename.upper():
            return Domain.GLOBAL
        else:
            logger.error("Unknown domain for file %s: %s", self.filename)
            raise ValueError(f"Unknown domain for file {self.filename}")

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
