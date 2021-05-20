from dataclasses import dataclass
from datetime import timedelta
from typing import Optional


@dataclass
class Rule(object):
    # gen_second param
    second: int = 0
    minute: int = 0
    hour: int = 0
    day: int = 0
    week: int = 0

    # token config
    max_token: int = 100  # Maximum number of tokens per bucket
    gen_token: int = 1  # The number of tokens generated per unit time
    init_token: Optional[int] = None  # The initial number of tokens in the bucket

    group: Optional[str] = None
    block_time: Optional[int] = None

    total_second: float = 0
    rate: float = 0

    def __post_init__(self) -> None:
        if not self.init_token:
            self.init_token = self.max_token

        # How long does it take to generate token
        self.total_second = timedelta(
            weeks=self.week, days=self.day, hours=self.hour, minutes=self.minute, seconds=self.second
        ).total_seconds()

        # total_second: 60 gen_token: 1  = 1 req/m
        # total_second: 1  gen_token: 1000 = 1000 req/s = 1 req/ms
        self.rate = self.gen_token / self.total_second
