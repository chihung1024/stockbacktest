import { Router, IRequest, error, json } from 'itty-router';

// --- 類型定義 ---
export interface Env {
	STOCK_DATA_BUCKET: R2Bucket;
}
interface PortfolioConfig {
    name: string;
    tickers: string[];
    weights: number[];
    rebalancingPeriod: 'never' | 'annually' | 'quarterly' | 'monthly';
}
interface BacktestPayload {
    portfolios: PortfolioConfig[];
    initialAmount: number;
    startYear: string;
    startMonth: string;
    endYear: string;
    endMonth: string;
    benchmark: string;
}
type PriceHistory = { date: string; value: number }[];

// --- 常數定義 ---
const TRADING_DAYS_PER_YEAR = 252;
const RISK_FREE_RATE = 0.0;
const EPSILON = 1e-9;

// --- 主路由器 ---
// itty-router 會自動處理 /api 前綴，因為檔案路徑是 /functions/api/[[path]].ts
const router = Router(); 

// --- 輔助函式：為最終回應添加標頭 ---
const finalizeResponse = (response: Response) => {
    const newHeaders = new Headers(response.headers);
    newHeaders.set('Access-Control-Allow-Origin', '*');
    newHeaders.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    newHeaders.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    return new Response(response.body, { ...response, headers: newHeaders });
};


// --- API 路由 ---
router.post('/backtest', async (request: IRequest, env: Env) => {
    try {
        const payload: BacktestPayload = await request.json();
        
        const allTickers = new Set<string>();
        payload.portfolios.forEach(p => p.tickers.forEach(t => allTickers.add(t)));
        if (payload.benchmark) {
            allTickers.add(payload.benchmark);
        }

        const rawPriceData: { [ticker: string]: Map<string, number> } = {};
        const missingTickers: string[] = [];

        // 優化：平行地從 R2 獲取所有需要的 CSV 檔案
        const promises = Array.from(allTickers).map(async (ticker) => {
            const object = await env.STOCK_DATA_BUCKET.get(`prices/${ticker}.csv`);
            if (object === null) {
                // 如果在 R2 找不到檔案，就記錄下來
                missingTickers.push(ticker);
            } else {
                const csvText = await object.text();
                rawPriceData[ticker] = parseCsvToMap(csvText);
            }
        });
        await Promise.all(promises);

        // 如果有任何股票資料缺失，就回傳一個清晰的錯誤訊息
        if (missingTickers.length > 0) {
            return error(400, `無法在 R2 儲存體中找到以下股票的數據，請檢查資料更新流程: ${missingTickers.join(', ')}`);
        }

        const alignedPriceData = createAlignedPriceData(rawPriceData, payload);
        if (alignedPriceData.dates.length < 2) {
             return error(400, '在指定的時間範圍內，找不到足夠的共同交易日來進行回測。');
        }

        const benchmarkHistory = payload.benchmark ? runSingleAssetSimulation(payload.benchmark, alignedPriceData, payload.initialAmount) : null;
        
        const resultsData = payload.portfolios.map(p => {
            return runPortfolioSimulation(p, alignedPriceData, payload.initialAmount, benchmarkHistory);
        });

        const benchmarkResult = benchmarkHistory ? {
            name: payload.benchmark,
            ...calculateMetrics(benchmarkHistory, null, RISK_FREE_RATE),
            beta: 1.0,
            alpha: 0.0,
            portfolioHistory: benchmarkHistory
        } : null;
        
        return json({
            data: resultsData,
            benchmark: benchmarkResult,
            warning: null
        });

    } catch (e: any) {
        console.error("回測時發生錯誤:", e.stack);
        return error(500, `伺服器內部錯誤: ${e.message}`);
    }
});

// --- 核心邏輯函式 (保持不變) ---

function parseCsvToMap(csvText: string): Map<string, number> {
    const lines = csvText.trim().split('\n').slice(1);
    const priceMap = new Map<string, number>();
    for (const line of lines) {
        const [date, price] = line.split(',');
        if (date && price) priceMap.set(date, parseFloat(price));
    }
    return priceMap;
}

function createAlignedPriceData(rawPriceData: { [ticker: string]: Map<string, number> }, payload: BacktestPayload) {
    const allDates = new Set<string>();
    Object.values(rawPriceData).forEach(priceMap => { priceMap.forEach((_, date) => allDates.add(date)); });
    const startDate = `${payload.startYear}-${payload.startMonth.padStart(2, '0')}-01`;
    const endDate = new Date(parseInt(payload.endYear), parseInt(payload.endMonth), 0).toISOString().split('T')[0];
    const sortedDates = Array.from(allDates).filter(date => date >= startDate && date <= endDate).sort();
    const prices: { [ticker: string]: (number | null)[] } = {};
    const tickers = Object.keys(rawPriceData);
    tickers.forEach(ticker => { prices[ticker] = sortedDates.map(date => rawPriceData[ticker]?.get(date) || null); });
    const alignedDates: string[] = [];
    const alignedPrices: { [ticker:string]: number[] } = {};
    tickers.forEach(ticker => alignedPrices[ticker] = []);
    for (let i = 0; i < sortedDates.length; i++) {
        if (tickers.every(ticker => prices[ticker][i] !== null)) {
            alignedDates.push(sortedDates[i]);
            tickers.forEach(ticker => { alignedPrices[ticker].push(prices[ticker][i]!); });
        }
    }
    return { dates: alignedDates, prices: alignedPrices };
}

function runSingleAssetSimulation(ticker: string, alignedData: any, initialAmount: number): PriceHistory {
    const prices = alignedData.prices[ticker];
    if (!prices || prices.length === 0) return [];
    const initialPrice = prices[0];
    const shares = initialAmount / initialPrice;
    return prices.map((price, i) => ({ date: alignedData.dates[i], value: price * shares }));
}

function runPortfolioSimulation(portfolio: PortfolioConfig, alignedData: any, initialAmount: number, benchmarkHistory: PriceHistory | null) {
    const { tickers, weights, rebalancingPeriod } = portfolio;
    const portfolioHistory: PriceHistory = [];
    if (tickers.length === 0 || alignedData.dates.length === 0) {
        return { name: portfolio.name, ...calculateMetrics([], null, RISK_FREE_RATE), portfolioHistory: [] };
    }
    const rebalancingDates = getRebalancingDates(alignedData.dates, rebalancingPeriod);
    const initialPrices = tickers.map(ticker => alignedData.prices[ticker][0]);
    const normalizedWeights = weights.map(w => w / 100);
    let shares = normalizedWeights.map((w, i) => (initialAmount * w) / (initialPrices[i] + EPSILON));
    for (let i = 0; i < alignedData.dates.length; i++) {
        const currentDate = alignedData.dates[i];
        const currentPrices = tickers.map(ticker => alignedData.prices[ticker][i]);
        const currentValue = shares.reduce((sum, s, j) => sum + s * currentPrices[j], 0);
        portfolioHistory.push({ date: currentDate, value: currentValue });
        if (rebalancingDates.has(currentDate)) {
             shares = normalizedWeights.map((w, j) => (currentValue * w) / (currentPrices[j] + EPSILON));
        }
    }
    const metrics = calculateMetrics(portfolioHistory, benchmarkHistory, RISK_FREE_RATE);
    return { name: portfolio.name, ...metrics, portfolioHistory };
}

function getRebalancingDates(dates: string[], period: string): Set<string> {
    if (period === 'never') return new Set();
    const rebalanceDates = new Set<string>();
    let lastMarker: string | null = null;
    for (const date of dates) {
        let currentMarker: string;
        const d = new Date(date);
        if (period === 'annually') currentMarker = `${d.getUTCFullYear()}`;
        else if (period === 'quarterly') currentMarker = `${d.getUTCFullYear()}-Q${Math.floor(d.getUTCMonth() / 3) + 1}`;
        else if (period === 'monthly') currentMarker = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
        else continue;
        if (currentMarker !== lastMarker) {
            rebalanceDates.add(date);
            lastMarker = currentMarker;
        }
    }
    if (dates.length > 0) rebalanceDates.delete(dates[0]);
    return rebalanceDates;
}

function calculateMetrics(portfolioHistory: PriceHistory, benchmarkHistory: PriceHistory | null, riskFreeRate: number): any {
    if (portfolioHistory.length < 2) return { cagr: 0, mdd: 0, volatility: 0, sharpe_ratio: 0, sortino_ratio: 0, beta: null, alpha: null };
    const startValue = portfolioHistory[0].value;
    const endValue = portfolioHistory[portfolioHistory.length - 1].value;
    if (startValue < EPSILON) return { cagr: 0, mdd: -1, volatility: 0, sharpe_ratio: 0, sortino_ratio: 0, beta: null, alpha: null };
    const startDate = new Date(portfolioHistory[0].date);
    const endDate = new Date(portfolioHistory[portfolioHistory.length - 1].date);
    const years = (endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24 * 365.25);
    const cagr = years > 0 ? Math.pow(endValue / startValue, 1 / years) - 1 : 0;
    let peak = -Infinity, mdd = 0;
    portfolioHistory.forEach(p => {
        peak = Math.max(peak, p.value);
        mdd = Math.min(mdd, (p.value - peak) / (peak + EPSILON));
    });
    const dailyReturns = [];
    for (let i = 1; i < portfolioHistory.length; i++) dailyReturns.push(portfolioHistory[i].value / portfolioHistory[i - 1].value - 1);
    if (dailyReturns.length < 2) return { cagr, mdd, volatility: 0, sharpe_ratio: 0, sortino_ratio: 0, beta: null, alpha: null };
    const meanReturn = dailyReturns.reduce((a, b) => a + b, 0) / dailyReturns.length;
    const std = Math.sqrt(dailyReturns.map(x => Math.pow(x - meanReturn, 2)).reduce((a, b) => a + b, 0) / (dailyReturns.length - 1));
    const volatility = std * Math.sqrt(TRADING_DAYS_PER_YEAR);
    const annualizedExcessReturn = cagr - riskFreeRate;
    const sharpe_ratio = annualizedExcessReturn / (volatility + EPSILON);
    const downsideReturns = dailyReturns.filter(r => r < 0);
    const downsideStd = downsideReturns.length > 1 ? Math.sqrt(downsideReturns.map(x => Math.pow(x, 2)).reduce((a, b) => a + b, 0) / downsideReturns.length) * Math.sqrt(TRADING_DAYS_PER_YEAR) : 0;
    const sortino_ratio = annualizedExcessReturn / (downsideStd + EPSILON);
    let beta = null, alpha = null;
    if (benchmarkHistory && benchmarkHistory.length > 1) {
        const benchReturns = [];
        for (let i = 1; i < benchmarkHistory.length; i++) benchReturns.push(benchmarkHistory[i].value / benchmarkHistory[i - 1].value - 1);
        const alignedLength = Math.min(dailyReturns.length, benchReturns.length);
        const pReturns = dailyReturns.slice(0, alignedLength);
        const bReturns = benchReturns.slice(0, alignedLength);
        const cov = covariance(pReturns, bReturns);
        const variance = variance_p(bReturns);
        if (variance > EPSILON) {
            beta = cov / variance;
            const benchStartValue = benchmarkHistory[0].value;
            const benchEndValue = benchmarkHistory[benchmarkHistory.length - 1].value;
            const benchCagr = years > 0 ? Math.pow(benchEndValue / benchStartValue, 1 / years) - 1 : 0;
            const expectedReturn = riskFreeRate + beta * (benchCagr - riskFreeRate);
            alpha = cagr - expectedReturn;
        }
    }
    return { cagr, mdd, volatility, sharpe_ratio, sortino_ratio, beta, alpha };
}

function variance_p(arr: number[]): number {
    if (arr.length < 2) return 0;
    const mean = arr.reduce((a,b) => a + b, 0) / arr.length;
    return arr.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / arr.length;
}
function covariance(arr1: number[], arr2: number[]): number {
    if (arr1.length !== arr2.length || arr1.length < 2) return 0;
    const mean1 = arr1.reduce((a,b) => a + b, 0) / arr1.length;
    const mean2 = arr2.reduce((a,b) => a + b, 0) / arr2.length;
    let cov = 0;
    for (let i = 0; i < arr1.length; i++) cov += (arr1[i] - mean1) * (arr2[i] - mean2);
    return cov / arr1.length;
}

// --- 路由器設定 ---
router.all('*', (req) => {
    if (req.method === 'OPTIONS') {
        const headers = new Headers({
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        });
        return new Response(null, { headers });
    }
});
router.all('*', () => error(404, '路由未找到 Not Found'));

// --- Cloudflare Pages Functions 的進入點 ---
export const onRequest: PagesFunction<Env> = async (context) => {
    // 由於檔案路徑是 /functions/api/[[path]].ts，itty-router 會自動處理 /api 前綴
    // 我們需要手動將 /api 從 URL 中移除，才能讓 itty-router 正確匹配 /backtest
    const url = new URL(context.request.url);
    url.pathname = url.pathname.replace(/^\/api/, '');
    const request = new Request(url, context.request);

    return router
        .handle(request, context.env, context)
        .catch((err) => {
            console.error("在路由器中捕獲到未處理的異常:", err);
            return error(500, (err as Error).message || '伺服器發生未知錯誤');
        })
        .then(finalizeResponse);
};
```

#### 2. 真正解決資料問題

在您更新並部署了上述程式碼後，接下來請務必檢查您的資料更新流程。

* **檢查 GitHub Actions:**
    1.  前往您的 GitHub repository，點擊上方的 **"Actions"** 標籤頁。
    2.  在左側選擇 **"Update Stock Data (Matrix Strategy)"** 這個工作流。
    3.  查看最近的執行紀錄。如果看到有紅色的 `X`，就表示執行失敗了。點進去看日誌，找出失敗的原因（很可能是 `yfinance` 抓不到資料）。
    4.  您可以點擊 "Run workflow" 按鈕手動執行一次，來獲取最新的資料並填充到 R2。

完成這兩步後，您的回測系統就應該能完全正常運
