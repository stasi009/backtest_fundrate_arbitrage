from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    init_cash: float
    margin_rate: float
    commission: float
    slippage: float

    ordersize_usd: float
    fundrate_diff_open: float  # funding rate diff > this threshold, open trades
    fundrate_diff_close: float  # funding rate diff < this threshold, close trades
    fundrate_diff_change_pct: float  # fundrate_diff的相对变化超过这个门槛，加仓或换仓

    data_dir: Path
    exchanges: list[str]
    markets: list[str]
