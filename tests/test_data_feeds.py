import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


def mock_dataframe(cex: str, num_rows: int, symbols: list[str]):
    rows = []

    start_tm = datetime(2014, 1, 1, 0, 0, 0)
    for r in range(num_rows):
        row = {"timestamp": start_tm + timedelta(hours=r)}

        for symbol in symbols:
            row[symbol + "_price"] = r
            row[symbol + "_fundrate"] = r

        rows.append(row)

    data_folder = Path("data/test")
    data_folder.mkdir(exist_ok=True)
    pd.DataFrame(rows).to_csv(data_folder / f"{cex}.csv")
    print(f'CEX[{cex}] mock data is dumped')


def create_mock_datas():
    symbols = ["A", "B", "C"]
    for cex in ["binance", "okx", "bitget"]:
        mock_dataframe(cex=cex, num_rows=10, symbols=symbols)
        

if __name__ == "__main__":
    create_mock_datas()
