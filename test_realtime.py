from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import time

tv = TvDatafeed()


def get_full_history(
    symbol="BTC.D", exchange="CRYPTOCAP", interval=Interval.in_1_minute, save_csv=True
):
    """
    Tải toàn bộ dữ liệu lịch sử từ TradingView theo interval.
    - symbol: mã tài sản (vd: BTC.D cho BTC Dominance)
    - exchange: CRYPTOCAP (TradingView định nghĩa cho dominance & marketcap)
    - interval: khung thời gian (vd: Interval.in_1_minute)
    - save_csv: có lưu ra file csv không
    """
    all_data = pd.DataFrame()
    to = None  # None = latest

    while True:
        # tải 1 chunk 5000 bars
        df = tv.get_hist(symbol, exchange, interval, n_bars=5000, to=to)
        if df is None or df.empty:
            print("Hết dữ liệu hoặc không tải thêm được.")
            break

        all_data = pd.concat([df, all_data])  # nối vào DataFrame chính
        print(f"Đã tải thêm {len(df)} bars, tổng cộng: {len(all_data)}")

        # dịch mốc thời gian về quá khứ
        to = df.index[0]

        if len(df) < 5000:
            print("Tới mốc dữ liệu cũ nhất rồi.")
            break

        time.sleep(1)  # tránh bị block

    all_data = all_data[~all_data.index.duplicated()]
    all_data.sort_index(inplace=True)

    if save_csv:
        filename = f"BTCD_{interval.name}.csv"
        all_data.to_csv(filename)
        print(f"Đã lưu file CSV: {filename}")

    return all_data


# Chạy thử: lấy toàn bộ BTC Dominance (BTC.D) khung 1 phút
df = get_full_history("BTC.D", "CRYPTOCAP", Interval.in_1_minute)

print(df.head())
print(df.tail())
print("Tổng số dòng:", len(df))
