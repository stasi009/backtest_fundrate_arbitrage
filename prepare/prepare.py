import subprocess
from datetime import datetime
import typer
from typing import Annotated
import time


class PrepareJob:
    def __init__(self) -> None:
        self._failed_commands = []

    def __download(self, exchange: str, coin: str, start_dt: str, end_dt: str):
        market = coin + "-USD"
        command = ["python", '-m', f"prepare.download_{exchange}", market, start_dt, end_dt]
        cmd_line = " ".join(command)

        try_counter = 1
        while try_counter <= 5:  # 有时会发生connetion issue，重试一次
            print(f"\n------ [{try_counter}] {cmd_line}")

            proc = subprocess.run(command)
            if proc.returncode == 0:
                typer.echo(
                    typer.style(f"++++++ [SUCCESS] {cmd_line}", fg=typer.colors.BRIGHT_GREEN, bold=True)
                )
                return

            try_counter += 1
            time.sleep(1)

        typer.echo(typer.style(f"!!!!!! [FAIL] {cmd_line}", fg=typer.colors.BRIGHT_RED, bold=True))
        self._failed_commands.append(cmd_line)

    def run(self, exchanges: str, coins: str, start_dt: datetime, end_dt: datetime):
        exchanges = [s.strip() for s in exchanges.split(",")]
        print(f"download from [{len(exchanges)}] exchanges: {exchanges}")

        coins = [s.strip().upper() for s in coins.split(",")]
        print(f"download [{len(coins)}] coins' funding rates: {coins}")

        start_dt = start_dt.strftime("%Y-%m-%d")
        end_dt = end_dt.strftime("%Y-%m-%d")

        for coin in coins:
            for exchange in exchanges:
                self.__download(exchange=exchange, coin=coin, start_dt=start_dt, end_dt=end_dt)

        if len(self._failed_commands) > 0:
            print(f"there are {len(self._failed_commands)} failed commands")
            for idx, cmdline in enumerate(self._failed_commands, start=1):
                print(f"[{idx:02d}] {cmdline}")


def main(exchanges: str, coins: str, start_dt: datetime, end_dt: datetime):
    downloader = PrepareJob()
    downloader.run(exchanges=exchanges, coins=coins, start_dt=start_dt, end_dt=end_dt)


if __name__ == "__main__":
    typer.run(main)
