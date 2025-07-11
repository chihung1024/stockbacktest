import pandas as pd
import yfinance as yf
import json
import time
import sys # 匯入 sys 模組
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# --- 設定資料輸出路徑 ---
output_folder = Path("dist")
prices_folder = output_folder / "prices"
output_folder.mkdir(exist_ok=True)
prices_folder.mkdir(exist_ok=True)
PREPROCESSED_JSON_PATH = output_folder / "preprocessed_data.json"

MAX_WORKERS = 20

# --- 數據源獲取函式 (加入更詳細的錯誤日誌) ---
def get_etf_holdings(etf_ticker):
    try:
        etf = yf.Ticker(etf_ticker)
        holdings = etf.holdings
        if holdings is not None and not holdings.empty:
            print(f"成功從 yfinance 獲取 {etf_ticker} 的成分股。")
            return holdings['symbol'].tolist()
        print(f"警告：yfinance 回傳了 {etf_ticker} 的空成分股列表。")
        return []
    except Exception as e:
        print(f"錯誤：從 yfinance 獲取 {etf_ticker} 成分股時失敗: {e}")
        return []

def get_sp500_from_wiki():
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        print("成功從維基百科獲取 S&P 500 成分股。")
        return tables[0]['Symbol'].str.replace('.', '-', regex=False).tolist()
    except Exception as e:
        print(f"錯誤：從維基百科獲取 S&P 500 成分股時失敗: {e}")
        return []

# --- 其他函式維持不變 ---
def fetch_stock_info(ticker):
    try:
        ticker_obj = yf.Ticker(ticker)
        info = ticker_obj.info
        if info.get('trailingPE') is None and info.get('marketCap') is None:
            return None
        return {
            'ticker': ticker, 'marketCap': info.get('marketCap'), 'sector': info.get('sector'),
            'trailingPE': info.get('trailingPE'), 'forwardPE': info.get('forwardPE'),
            'dividendYield': info.get('dividendYield'), 'returnOnEquity': info.get('returnOnEquity'),
            'revenueGrowth': info.get('revenueGrowth'), 'earningsGrowth': info.get('earningsGrowth')
        }
    except Exception:
        return None

def fetch_price_history(ticker):
    try:
        data = yf.download(ticker, start="1990-01-01", auto_adjust=True, progress=False)
        if not data.empty:
            price_df = data[['Close']].copy()
            price_df.to_csv(prices_folder / f"{ticker}.csv")
            return ticker, True
        return ticker, False
    except Exception:
        return ticker, False

# --- 主執行函式 (加入最終檢查) ---
def main():
    print("--- 開始獲取指數成分股列表 ---")
    sp500_tickers = get_sp500_from_wiki()
    # 如果維基百科失敗，嘗試從 ETF 獲取作為備案
    if not sp500_tickers:
        print("維基百科獲取失敗，嘗試從 VOO ETF 獲取...")
        sp500_tickers = get_etf_holdings("VOO")
        
    nasdaq100_tickers = get_etf_holdings("QQQ")
    
    all_unique_tickers = sorted(list(set(sp500_tickers).union(set(nasdaq_tickers))))

    if not all_unique_tickers:
        print("致命錯誤：所有數據來源均無法獲取任何成分股，終止執行。")
        sys.exit(1) # 讓腳本以錯誤狀態結束，GitHub Actions 會知道這一步失敗了
        
    print(f"總共找到 {len(all_unique_tickers)} 支不重複的股票。")

    print("\n--- 步驟 1/2: 平行下載基本面數據 ---")
    all_stock_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(fetch_stock_info, ticker): ticker for ticker in all_unique_tickers}
        for future in tqdm(as_completed(future_to_ticker), total=len(all_unique_tickers), desc="獲取基本面"):
            info = future.result()
            if info:
                info['in_sp500'] = info['ticker'] in sp500_tickers
                info['in_nasdaq100'] = info['ticker'] in nasdaq100_tickers
                all_stock_data.append(info)

    if not all_stock_data:
        print("致命錯誤：無法獲取任何股票的基本面數據，終止執行。")
        sys.exit(1)

    with open(PREPROCESSED_JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_stock_data, f, ensure_ascii=False, indent=4)
    print(f"基本面數據處理完成，共獲取 {len(all_stock_data)} 筆有效資料。")

    print("\n--- 步驟 2/2: 平行下載歷史價格數據 ---")
    success_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(fetch_price_history, ticker): ticker for ticker in all_unique_tickers}
        for future in tqdm(as_completed(future_to_ticker), total=len(all_unique_tickers), desc="下載價格"):
            _ticker, success = future.result()
            if success:
                success_count += 1
    
    if success_count == 0:
        print("致命錯誤：無法下載任何股票的歷史價格數據，終止執行。")
        sys.exit(1)
        
    print(f"歷史價格數據更新完成，共成功下載 {success_count} 支股票。")

if __name__ == '__main__':
    main()
