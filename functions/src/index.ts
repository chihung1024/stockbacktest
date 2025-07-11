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

interface PriceData {
    [ticker: string]: { date: string; price: number }[];
}

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

        // 2. 從 R2 平行讀取並解析所有 CSV 檔案
        const priceData: PriceData = {};
        const promises = Array.from(allTickers).map(async (ticker) => {
            const object = await env.STOCK_DATA_BUCKET.get(`prices/${ticker}.csv`);
            if (!object) {
                console.warn(`在 R2 中找不到 ${ticker}.csv`);
                return;
            };
            const csvText = await object.text();
            priceData[ticker] = parseCsv(csvText);
        });
        await Promise.all(promises);

        // 3. 執行回測模擬
        // TODO: 這是最核心的邏輯，需要將 Python 的 pandas 數據對齊和計算邏輯翻譯過來
        const results = runSimulation(payload, priceData);
        
        // 4. 回傳結果
        return json(results);

    } catch (e: any) {
        console.error("回測時發生錯誤:", e);
        return error(500, `伺服器內部錯誤: ${e.message}`);
    }
});

// 處理所有其他路由
router.all('*', () => error(404, 'Not Found'));

// --- 核心邏輯函式 ---

/**
 * 解析 CSV 文字檔，轉換成價格資料陣列
 * @param csvText - 從 R2 讀取的 CSV 內容
 * @returns 價格資料陣列
 */
function parseCsv(csvText: string): { date: string; price: number }[] {
    const lines = csvText.trim().split('\n');
    const header = lines.shift(); // 移除標題行
    return lines.map(line => {
        const [date, price] = line.split(',');
        return { date, price: parseFloat(price) };
    });
}

/**
 * 執行回測模擬的主函式
 * @param payload - 前端傳來的請求內容
 * @param priceData - 從 R2 讀取並解析好的所有價格資料
 * @returns 計算完成的回測結果
 */
function runSimulation(payload: BacktestPayload, priceData: PriceData) {
    // --- 這是核心計算邏輯的起點 ---
    // 下方的實作是一個簡化的框架，它會回傳有效的 JSON 結構以解決前端錯誤，
    // 但計算結果是預設的。您需要在此基礎上，逐步實現 Python 版本的完整計算邏輯。

    const resultsData = payload.portfolios.map(p => {
        // 模擬的投資組合歷史
        const portfolioHistory = [
            { date: `${payload.startYear}-${payload.startMonth.padStart(2, '0')}-01`, value: payload.initialAmount },
            { date: `${payload.endYear}-${payload.endMonth.padStart(2, '0')}-01`, value: payload.initialAmount * (1 + Math.random()) } // 隨機產生一個結束值
        ];
        
        // 模擬的績效指標
        const metrics = calculateMetrics(portfolioHistory);

        return {
            name: p.name,
            ...metrics,
            portfolioHistory
        };
    });

    // 模擬的比較基準結果
    const benchmarkResult = {
        name: payload.benchmark,
        ...calculateMetrics([
             { date: `${payload.startYear}-${payload.startMonth.padStart(2, '0')}-01`, value: payload.initialAmount },
             { date: `${payload.endYear}-${payload.endMonth.padStart(2, '0')}-01`, value: payload.initialAmount * (1 + Math.random()) }
        ]),
        portfolioHistory: [
             { date: `${payload.startYear}-${payload.startMonth.padStart(2, '0')}-01`, value: payload.initialAmount },
             { date: `${payload.endYear}-${payload.endMonth.padStart(2, '0')}-01`, value: payload.initialAmount * (1 + Math.random()) }
        ]
    };
    
    return {
        data: resultsData,
        benchmark: benchmarkResult,
        warning: "請注意：後端計算邏輯尚未完全實現，目前顯示的是模擬數據。"
    };
}

/**
 * 計算績效指標的函式
 * @param portfolioHistory - 投資組合的價值歷史
 * @returns 各項績效指標
 */
function calculateMetrics(portfolioHistory: { date: string; value: number }[]): object {
    // TODO: 在這裡實現 CAGR, MDD, Sharpe, Sortino, Beta, Alpha 的計算邏輯
    // 目前回傳預設值
    return {
        cagr: 0.15 + (Math.random() * 0.1 - 0.05), // 隨機產生一個 10% ~ 20% 的值
        mdd: -0.20 + (Math.random() * 0.1 - 0.05), // 隨機產生一個 -15% ~ -25% 的值
        volatility: 0.18 + (Math.random() * 0.1 - 0.05),
        sharpe_ratio: 0.8 + (Math.random() * 0.4 - 0.2),
        sortino_ratio: 1.2 + (Math.random() * 0.5 - 0.25),
        beta: 1.0,
        alpha: 0.01 + (Math.random() * 0.02 - 0.01),
    };
}


// --- Cloudflare Worker 的進入點 ---
export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        return router.handle(request, env, ctx)
            .then(response => {
                // 為所有成功的回應加上 CORS header
                const newHeaders = new Headers(response.headers);
                newHeaders.set('Access-Control-Allow-Origin', '*');
                return new Response(response.body, { ...response, headers: newHeaders });
            })
            .catch(err => {
                // 為錯誤回應加上 CORS header
                const response = error(err.status || 500, err.message);
                const newHeaders = new Headers(response.headers);
                newHeaders.set('Access-Control-Allow-Origin', '*');
                return new Response(response.body, { ...response, headers: newHeaders });
            });
    },
};
