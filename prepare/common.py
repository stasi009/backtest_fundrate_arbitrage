from enum import Enum
from datetime import datetime, timezone
import httpx
import typer
from pathlib import Path

UTC_TM_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def check_http_error(response: httpx.Response) -> None:
    # print(f"response status: {response.status_code}")
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        err_msg = typer.style(f"!!! Error[{response.status_code}]: {response.json()}", fg=typer.colors.RED)
        typer.echo(err_msg)
        raise typer.Exit(code=1)


def safe_output_path(outfname: str | Path):
    if isinstance(outfname, str):
        outfname = Path(outfname)
    outfname.parent.mkdir(parents=True, exist_ok=True)
    return outfname


def raw_data_file(exchange: str, market: str):
    return f"data/raw/{exchange}_{market}.csv"
