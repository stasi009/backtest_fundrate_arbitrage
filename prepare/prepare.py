import subprocess
from datetime import datetime, time, timezone
import pandas as pd
import typer
from prepare.common import raw_data_file, safe_output_path


class PrepareJob:
    def __init__(self, exchanges: str, coins: str, start_dt: datetime, end_dt: datetime) -> None:
        self.__exchanges = [s.strip() for s in exchanges.split(",")]
        self.__markets = [s.strip().upper() + "-USD" for s in coins.split(",")]
        self.__start_dt = start_dt
        self.__end_dt = end_dt
        self.__datetime_index = None
        self.__failed_commands = []

    def __download(self, exchange: str, market: str, start_dt: str, end_dt: str):
        command = ["python", "-m", f"prepare.download_{exchange}", market, start_dt, end_dt]
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
        self.__failed_commands.append(cmd_line)

    def download(self):

        for market in self.__markets:
            for exchange in self.__exchanges:
                self.__download(
                    exchange=exchange,
                    market=market,
                    start_dt=self.__start_dt.strftime("%Y-%m-%d"),
                    end_dt=self.__end_dt.strftime("%Y-%m-%d"),
                )

        if len(self.__failed_commands) > 0:
            print(f"there are {len(self.__failed_commands)} failed commands")
            for idx, cmdline in enumerate(self.__failed_commands, start=1):
                print(f"[{idx:02d}] {cmdline}")

        return self.__failed_commands == 0

    def postprocess(self):
        self.__datetime_index = pd.date_range(
            start=datetime.combine(self.__start_dt.date(), time()),
            end=datetime.combine(self.__end_dt.date(), time(23, 59, 59)),
            freq="h",
        )

        datas = {}
        for exchange in self.__exchanges:
            for market in self.__markets:
                df = pd.read_csv(raw_data_file(exchange, market), index_col="timestamp", parse_dates=True)
                datas[(exchange, market)] = df.reindex(index=self.__datetime_index)

        # 最后一部分的文件名xxx.csv是占位符，整个操作就是保证data/input能够被正确创建
        safe_output_path("data/input/xxx.csv")

        for (exchange, market), data in datas.items():
            if "mark_price" not in data.columns:
                # 有的exchange不提供mark prices，就用dydx的mark price来代替
                mark_price = datas[("dydx", market)]["mark_price"]
                data = data.join(mark_price)

            # 保证行列都有序
            data = data.loc[:, ["fund_rate", "mark_price", "open_price", "close_price"]]
            data.sort_index(inplace=True)
            data.to_csv(f"data/input/{exchange}_{market}.csv", index_label="timestamp")
            print(f"backtest input {market}@{exchange} saved")


def main(exchanges: str, coins: str, start_dt: datetime, end_dt: datetime):
    downloader = PrepareJob(exchanges=exchanges, coins=coins, start_dt=start_dt, end_dt=end_dt)
    if downloader.download():
        downloader.postprocess()


if __name__ == "__main__":
    typer.run(main)
