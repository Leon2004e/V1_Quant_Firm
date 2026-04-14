from enum import Enum


class Broker(str, Enum):
    FTMO = "FTMO"
    DARWINEX = "DARWINEX"
    GENERIC = "GENERIC"


class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TradeStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class AssetClass(str, Enum):
    FX = "FX"
    INDEX = "INDEX"
    COMMODITY = "COMMODITY"
    CRYPTO = "CRYPTO"
    EQUITY = "EQUITY"
    UNKNOWN = "UNKNOWN"