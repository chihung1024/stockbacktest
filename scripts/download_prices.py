import pandas as pd
import yfinance as yf
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def fetch_price_history(ticker, prices_folder):
    """下載單支股票的歷史價格並儲存為 CSV"""
    try:
        data = yf.download(ticker, start="1990-01-01", auto_adjust=True, progress=False)
        if not data.empty:
            price_df = data[['Close']].copy()
            # 確保輸出資料夾存在
            prices_folder.mkdir(exist_ok=True)
            price_df.to_csv(prices_folder / f"{ticker}.csv")
            return True
        return False
    except Exception as e:
        print(f"下載 {ticker} 價格時發生錯誤: {e}")
        return False

def main(slice_index, total_slices):
    """主執行函式，處理特定分片的股票"""
    
    # --- 設定路徑 ---
    base_folder = Path(".")
    tickers_list_path = base_folder / "dist" / "tickers.txt"
    output_prices_folder = base_folder / "dist" / "prices"
    
    # 讀取完整的股票清單
    try:
        with open(tickers_list_path, 'r') as f:
            all_tickers = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"錯誤：找不到股票清單檔案 {tickers_list_path}。請先執行 metadata 更新作業。")
        sys.exit(1)

    # --- 計算此作業負責的分片 ---
    num_tickers = len(all_tickers)
    slice_size = (num_tickers + total_slices - 1) // total_slices # 確保整除
    start = slice_index * slice_size
    end = start + slice_size
    tickers_to_process = all_tickers[start:end]

    if not tickers_to_process:
        print(f"分片 {slice_index}/{total_slices} 沒有需要處理的股票，正常結束。")
        return

    print(f"--- 作業 {slice_index}/{total_slices}: 開始處理 {len(tickers_to_process)} 支股票 (從 {tickers_to_process[0]} 到 {tickers_to_process[-1]}) ---")
    
    # --- 平行下載價格 ---
    MAX_WORKERS = 10 # 在每個分片作業中，也可以使用多執行緒
    success_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(fetch_price_history, ticker, output_prices_folder): ticker for ticker in tickers_to_process}
        for future in tqdm(as_completed(future_to_ticker), total=len(tickers_to_process), desc=f"分片 {slice_index} 下載中"):
            if future.result():
                success_count += 1
    
    print(f"--- 作業 {slice_index}/{total_slices}: 處理完成，共成功下載 {success_count} 支股票。 ---")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("用法: python download_prices.py <slice_index> <total_slices>")
        sys.exit(1)
    
    slice_index = int(sys.argv[1])
    total_slices = int(sys.argv[2])
    main(slice_index, total_slices)
