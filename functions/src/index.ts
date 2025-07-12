import { Router, IRequest, error, json } from 'itty-router';

// --- 類型定義 (與之前相同) ---
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
const router = Router();

// 處理 CORS Preflight 請求
router.all('*', (request) => {
    if (request.method === 'OPTIONS') {
        return new Response(null, {
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            },
        });
    }
});

// --- API 路由 ---
router.post('/api/backtest', async (request: IRequest, env: Env) => {
    try {
        const payload: BacktestPayload = await request.json();
        
        // 1. 收集所有需要的股票代碼
        const allTickers = new Set<string>();
        payload.portfolios.forEach(p => p.tickers.forEach(t => allTickers.add(t)));
        if (payload.benchmark) {
            allTickers.add(payload.benchmark);
        }

        // 2. 從 R2 平行讀取，若無則從 yfinance 即時獲取
        const rawPriceData: { [ticker: string]: Map<string, number> } = {};
        const promises = Array.from(allTickers).map(async (ticker) => {
            const object = await env.STOCK_DATA_BUCKET.get(`prices/${ticker}.csv`);
            let csvText: string | null;

            if (object === null) {
                // R2 中沒有或快取已過期，從 yfinance 即時獲取並存入 R2
                console.warn(`在 R2 中找不到 ${ticker}.csv 或快取已過期，將嘗試從 yfinance 即時獲取。`);
                csvText = await fetchAndCacheFromYfinance(ticker, env);
            } else {
                // 從 R2 讀取
                csvText = await object.text();
            }
            
            if (csvText) {
                rawPriceData[ticker] = parseCsvToMap(csvText);
            } else {
                // 如果兩個來源都失敗，則拋出錯誤
                throw new Error(`無法獲取股票 ${ticker} 的數據。`);
            }
        });
        await Promise.all(promises);

        // 3. 建立對齊後的價格 DataFrame
        const alignedPriceData = createAlignedPriceData(rawPriceData, payload);
        if (alignedPriceData.dates.length < 2) {
             return error(400, '在指定的時間範圍內，找不到足夠的共同交易日來進行回測。');
        }

        // 4. 執行回測模擬
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
        
        // 5. 回傳結果
        return json({
            data: resultsData,
            benchmark: benchmarkResult,
            warning: null
        });

    } catch (e: any) {
        console.error("回測時發生錯誤:", e);
        return error(500, `伺服器內部錯誤: ${e.message}`);
    }
});

// 處理所有其他路由
router.all('*', () => error(404, 'Not Found'));

// --- 核心邏輯函式 ---

/**
 * 從 yfinance 獲取數據，並存入 R2
 */
async function fetchAndCacheFromYfinance(ticker: string, env: Env): Promise<string | null> {
    const endDate = Math.floor(Date.now() / 1000);
    const startDate = 0; // 從最早的日期開始
    const yfinanceUrl = `https://query1.finance.yahoo.com/v7/finance/download/${ticker}?period1=${startDate}&period2=${endDate}&interval=1d&events=history&includeAdjustedClose=true`;

    try {
        console.log(`正在從 yfinance 獲取 ${ticker} 的數據...`);
        const response = await fetch(yfinanceUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        });

        if (!response.ok) {
            throw new Error(`yfinance API 回傳錯誤: ${response.status} ${response.statusText}`);
        }

        const csvText = await response.text();
        if (!csvText || !csvText.startsWith('Date,Open,High,Low,Close,Adj Close,Volume')) {
             throw new Error(`yfinance 回傳的不是有效的 CSV 數據 for ${ticker}`);
        }

        const lines = csvText.trim().split('\n');
        const header = lines.shift()!.split(',');
        const dateIndex = header.indexOf('Date');
        const adjCloseIndex = header.indexOf('Adj Close');

        if (dateIndex === -1 || adjCloseIndex === -1) {
            throw new Error(`CSV 標頭中找不到 'Date' 或 'Adj Close' for ${ticker}`);
        }
        
        const reformattedCsvLines = ['Date,Close'];
        for (const line of lines) {
            const values = line.split(',');
            reformattedCsvLines.push(`${values[dateIndex]},${values[adjCloseIndex]}`);
        }
        const reformattedCsvText = reformattedCsvLines.join('\n');

        // 將數據存入 R2，並設定 4 小時的快取過期時間
        // 這樣可以確保非核心標的（如台股）的數據不會過於陳舊
        await env.STOCK_DATA_BUCKET.put(`prices/${ticker}.csv`, reformattedCsvText, {
            httpMetadata: {
                contentType: 'text/csv',
                cacheExpiry: new Date(Date.now() + 4 * 60 * 60 * 1000), // 4 hours from now
            },
        });
        console.log(`已成功獲取 ${ticker} 的數據並存入 R2 (4小時後過期)。`);

        return reformattedCsvText;
    } catch (e: any) {
        console.error(`從 yfinance 獲取 ${ticker} 失敗:`, e.message);
        return null;
    }
}


/**
 * 解析 CSV 文字檔，轉換成日期到價格的 Map
 */
function parseCsvToMap(csvText: string): Map<string, number> {
    const lines = csvText.trim().split('\n').slice(1);
    const priceMap = new Map<string, number>();
    for (const line of lines) {
        const [date, price] = line.split(',');
        if (date && price) {
            priceMap.set(date, parseFloat(price));
        }
    }
    return priceMap;
}

/**
 * 建立對齊後的價格 DataFrame
 */
function createAlignedPriceData(rawPriceData: { [ticker: string]: Map<string, number> }, payload: BacktestPayload) {
    const allDates = new Set<string>();
    Object.values(rawPriceData).forEach(priceMap => {
        priceMap.forEach((_, date) => allDates.add(date));
    });

    const startDate = `${payload.startYear}-${payload.startMonth.padStart(2, '0')}-01`;
    const endDate = new Date(parseInt(payload.endYear), parseInt(payload.endMonth), 0).toISOString().split('T')[0];

    const sortedDates = Array.from(allDates)
        .filter(date => date >= startDate && date <= endDate)
        .sort();

    const prices: { [ticker: string]: (number | null)[] } = {};
    const tickers = Object.keys(rawPriceData);
    tickers.forEach(ticker => {
        prices[ticker] = sortedDates.map(date => rawPriceData[ticker].get(date) || null);
    });

    const alignedDates: string[] = [];
    const alignedPrices: { [ticker: string]: number[] } = {};
    tickers.forEach(ticker => alignedPrices[ticker] = []);

    for (let i = 0; i < sortedDates.length; i++) {
        let allPricesExist = true;
        for (const ticker of tickers) {
            if (prices[ticker][i] === null) {
                allPricesExist = false;
                break;
            }
        }
        if (allPricesExist) {
            alignedDates.push(sortedDates[i]);
            for (const ticker of tickers) {
                alignedPrices[ticker].push(prices[ticker][i]!);
            }
        }
    }
    
    return { dates: alignedDates, prices: alignedPrices };
}


/**
 * 執行單一資產（如比較基準）的回測
 */
function runSingleAssetSimulation(ticker: string, alignedData: any, initialAmount: number): PriceHistory {
    const prices = alignedData.prices[ticker];
    if (!prices || prices.length === 0) return [];
    
    const initialPrice = prices[0];
    const shares = initialAmount / initialPrice;
    
    return prices.map((price, i) => ({
        date: alignedData.dates[i],
        value: price * shares,
    }));
}

/**
 * 執行投資組合回測的主函式
 */
function runPortfolioSimulation(portfolio: PortfolioConfig, alignedData: any, initialAmount: number, benchmarkHistory: PriceHistory | null) {
    const { tickers, weights, rebalancingPeriod } = portfolio;
    const portfolioHistory: PriceHistory = [];
    
    if (tickers.length === 0 || alignedData.dates.length === 0) {
        return { name: portfolio.name, ...calculateMetrics([]), portfolioHistory: [] };
    }

    const rebalancingDates = getRebalancingDates(alignedData.dates, rebalancingPeriod);
    let shares = new Array(tickers.length).fill(0);

    const initialPrices = tickers.map(ticker => alignedData.prices[ticker][0]);
    const normalizedWeights = weights.map(w => w / 100);
    shares = normalizedWeights.map((w, i) => (initialAmount * w) / (initialPrices[i] + EPSILON));

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
        
        if (period === 'annually') {
            currentMarker = `${d.getUTCFullYear()}`;
        } else if (period === 'quarterly') {
            currentMarker = `${d.getUTCFullYear()}-Q${Math.floor(d.getUTCMonth() / 3) + 1}`;
        } else if (period === 'monthly') {
            currentMarker = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
        } else {
            continue;
        }

        if (currentMarker !== lastMarker) {
            rebalanceDates.add(date);
            lastMarker = currentMarker;
        }
    }
    if (dates.length > 0) {
        rebalanceDates.delete(dates[0]);
    }
    return rebalanceDates;
}


/**
 * 計算績效指標的函式
 */
function calculateMetrics(portfolioHistory: PriceHistory, benchmarkHistory: PriceHistory | null, riskFreeRate: number): any {
    if (portfolioHistory.length < 2) {
        return { cagr: 0, mdd: 0, volatility: 0, sharpe_ratio: 0, sortino_ratio: 0, beta: null, alpha: null };
    }

    const startValue = portfolioHistory[0].value;
    const endValue = portfolioHistory[portfolioHistory.length - 1].value;
    
    if (startValue < EPSILON) return { cagr: 0, mdd: -1, volatility: 0, sharpe_ratio: 0, sortino_ratio: 0, beta: null, alpha: null };

    const startDate = new Date(portfolioHistory[0].date);
    const endDate = new Date(portfolioHistory[portfolioHistory.length - 1].date);
    const years = (endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24 * 365.25);
    const cagr = years > 0 ? Math.pow(endValue / startValue, 1 / years) - 1 : 0;

    let peak = -Infinity;
    let mdd = 0;
    portfolioHistory.forEach(p => {
        peak = Math.max(peak, p.value);
        const drawdown = (p.value - peak) / (peak + EPSILON);
        mdd = Math.min(mdd, drawdown);
    });

    const dailyReturns = [];
    for (let i = 1; i < portfolioHistory.length; i++) {
        dailyReturns.push(portfolioHistory[i].value / portfolioHistory[i - 1].value - 1);
    }
    
    if (dailyReturns.length < 2) {
         return { cagr, mdd, volatility: 0, sharpe_ratio: 0, sortino_ratio: 0, beta: null, alpha: null };
    }

    const meanReturn = dailyReturns.reduce((a, b) => a + b, 0) / dailyReturns.length;
    const std = Math.sqrt(dailyReturns.map(x => Math.pow(x - meanReturn, 2)).reduce((a, b) => a + b, 0) / (dailyReturns.length - 1));
    const volatility = std * Math.sqrt(TRADING_DAYS_PER_YEAR);

    const annualizedExcessReturn = cagr - riskFreeRate;
    const sharpe_ratio = annualizedExcessReturn / (volatility + EPSILON);

    const downsideReturns = dailyReturns.filter(r => r < 0);
    const downsideStd = downsideReturns.length > 1 ? Math.sqrt(downsideReturns.map(x => Math.pow(x, 2)).reduce((a, b) => a + b, 0) / downsideReturns.length) * Math.sqrt(TRADING_DAYS_PER_YEAR) : 0;
    const sortino_ratio = annualizedExcessReturn / (downsideStd + EPSILON);

    let beta = null;
    let alpha = null;

    if (benchmarkHistory && benchmarkHistory.length > 1) {
        const benchReturns = [];
        for (let i = 1; i < benchmarkHistory.length; i++) {
            benchReturns.push(benchmarkHistory[i].value / benchmarkHistory[i - 1].value - 1);
        }

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

// --- 數學輔助函式 ---
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
    for (let i = 0; i < arr1.length; i++) {
        cov += (arr1[i] - mean1) * (arr2[i] - mean2);
    }
    return cov / arr1.length;
}

// --- Cloudflare Worker 的進入點 ---
export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        return router.handle(request, env, ctx)
            .then(response => {
                const newHeaders = new Headers(response.headers);
                newHeaders.set('Access-Control-Allow-Origin', '*');
                return new Response(response.body, { ...response, headers: newHeaders });
            })
            .catch(err => {
                const response = error(err.status || 500, err.message);
                const newHeaders = new Headers(response.headers);
                newHeaders.set('Access-Control-Allow-Origin', '*');
                return new Response(response.body, { ...response, headers: newHeaders });
            });
    },
};
