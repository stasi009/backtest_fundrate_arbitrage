from enum import Enum
from datetime import datetime, timezone
import httpx
import typer
from pathlib import Path

UTC_TM_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class StableCoin(Enum):
    USDT = "USDT"
    USDC = "USDC"


def truncate_to_hour(txt: str) -> datetime:
    """
    尽管返回的都是小时级别的funding rate，但是返回结果的timestamp，由于误差原因，导致microsecond等位置存在non-zero
    这会导致未来无法按照时间匹配，所以这里强制将minute/second/microsecond都设置为0，确保未来匹配成功
    """
    dt = datetime.strptime(txt, UTC_TM_FORMAT)
    return dt.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)


def check_http_error(response: httpx.Response) -> None:
    # print(f"response status: {response.status_code}")
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        err_msg = typer.style(f"!!! Error[{response.status_code}]: {response.json()}", fg=typer.colors.RED)
        typer.echo(err_msg)
        raise typer.Exit(code=1)


def raw_data_path(exchange: str, market: str) -> Path:
    data_dir = Path(f"data/raw/{exchange.lower()}")
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / f"{market}.csv"


def safe_output_path(outfname: str | Path):
    if isinstance(outfname, str):
        outfname = Path(outfname)
    outfname.parent.mkdir(parents=True, exist_ok=True)
    return outfname


def load_all_coins():
    all_coins = []
    with open("coin_list.txt", "rt") as fin:
        for line in fin:
            if line.startswith("#"):
                continue
            line = line.strip()
            if len(line) == 0:
                continue
            all_coins.append(line)
    return all_coins
