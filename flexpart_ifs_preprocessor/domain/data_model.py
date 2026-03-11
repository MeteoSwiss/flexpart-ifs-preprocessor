import typing
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import re
from enum import Enum


logger = logging.getLogger(__name__)

class Stream(Enum):
    S4Y = "S4Y" # devt
    S5Y = "S5Y" # depl
    S6Y = "S6Y" # prod
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


class InputDataAggregatorEvent:

    def __init__(self, data: dict) -> None:

        self.object_key: str = data['objectStoreUuid']
        self.filename: str = data['fileName']
        self.stream = self._extract_stream()

        if self.stream != Stream.UNKNOWN:
            self.forecast_ref_time = self._extract_datetime()
            self.step = self._extract_lead_time()
            self.domain = self._extract_domain()
        else:
            self.forecast_ref_time = None
            self.step = None
            self.domain = None

    def _extract_stream(self) -> Stream:
        match = re.search(r'(S[456]Y)', self.filename.upper())
        if not match:
            return Stream.UNKNOWN
        return Stream(match.group(1))

    def _extract_domain(self) -> Domain:
        # F1 - Global domain
        if "_F1_" in self.filename.upper():
            return Domain.EUROPE
        # F2 - Europe domain
        elif "_F2_" in self.filename.upper():
            return Domain.GLOBAL
        else:
            logger.error("Unknown domain for file %s", self.filename)
            raise ValueError(f"Unknown domain for file {self.filename}")

    def _extract_datetime(self, s: str) -> datetime:
        match = re.search(r'(\d{8}T\d{6}Z)', s)
        if not match:
            raise ValueError(f"No datetime found in string: {s}")
        return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)

    def _extract_lead_time(self, s: str) -> int:
        match = re.search(r'_(\d+)([a-z]+)$', s)
        if not match:
            raise ValueError(f"No lead time found in string: {s}")
        value, unit = match.group(1), match.group(2)
        if unit != "h":
            raise ValueError(f"Expected unit 'h', got {unit}")
        return int(value)
