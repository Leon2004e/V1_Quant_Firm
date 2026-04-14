from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from core.enums import AssetClass, Broker, TradeSide, TradeStatus


@dataclass(frozen=True)
class TradeRecord:
    broker: Broker
    account_id: str
    ticket: int
    symbol: str
    side: TradeSide
    status: TradeStatus

    volume: float
    open_time: datetime
    open_price: float

    close_time: Optional[datetime]
    close_price: Optional[float]

    sl: Optional[float]
    tp: Optional[float]

    commission: float
    swap: float
    profit: float

    strategy_id: Optional[str] = None
    magic_number: Optional[int] = None
    comment: Optional[str] = None
    asset_class: AssetClass = AssetClass.UNKNOWN

    @property
    def is_open(self) -> bool:
        return self.status == TradeStatus.OPEN

    @property
    def is_closed(self) -> bool:
        return self.status == TradeStatus.CLOSED

    @property
    def net_pnl(self) -> float:
        return self.profit + self.commission + self.swap

    @property
    def holding_minutes(self) -> Optional[float]:
        if self.close_time is None:
            return None
        return (self.close_time - self.open_time).total_seconds() / 60.0