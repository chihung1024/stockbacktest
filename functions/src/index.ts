import { Router, IRequest, error, json } from 'itty-router';

// 定義環境變數的類型，用於綁定 R2
export interface Env {
	STOCK_DATA_BUCKET: R2Bucket;
}

const router = Router();

// 處理 CORS Preflight 請求
router.all('*', (request) => {
    if (request.method === 'OPTIONS') {
        const headers = new Headers({
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        });
        return new Response(null, { headers });
    }
});

// 處理回測請求
router.post('/api/backtest', async (request: IRequest, env: Env) => {
    try {
        const body = await request.json();
        
        // 重要提示：
        // 這裡需要將您原本在 Python 中的 `simulation.py` 和 `calculations.py` 核心邏輯
        // 翻譯成 TypeScript/JavaScript。這是一個複雜的數值計算過程。
        // 下方僅為一個示意性的框架，展示如何從 R2 讀取數據。
        
        const { portfolios, benchmark, startYear, startMonth, endYear, endMonth } = body;
        
        // 1. 收集所有需要的股票代碼
        const allTickers = new Set<string>();
        portfolios.forEach(p => p.tickers.forEach(t => allTickers.add(t)));
        if (benchmark) {
            allTickers.add(benchmark);
        }

        // 2. 從 R2 平行讀取所有 CSV 檔案
        const priceDataPromises = Array.from(allTickers).map(async (ticker) => {
            const object = await env.STOCK_DATA_BUCKET.get(`prices/${ticker}.csv`);
            if (!object) return { ticker, data: null };
            const text = await object.text();
            // TODO: 解析 CSV 文字，轉換成 { date, price } 的陣列
            return { ticker, data: text };
        });

        const priceDataResults = await Promise.all(priceDataPromises);

        // TODO: 
        // 3. 組合所有價格數據，找到共同日期範圍
        // 4. 根據 `rebalancingPeriod` 執行 `run_simulation` 的邏輯
        // 5. 執行 `calculate_metrics` 的邏輯
        // 6. 組織回傳的 JSON 結構

        // 暫時的回應，表示 API 已收到請求
        return json({ 
            message: "API endpoint is working. Calculation logic needs to be implemented.",
            receivedTickers: Array.from(allTickers),
            dataSnippets: priceDataResults.map(r => ({ ticker: r.ticker, snippet: r.data ? r.data.slice(0, 100) + '...' : 'Not Found' }))
        });

    } catch (e) {
        console.error(e);
        return error(500, '伺服器內部錯誤');
    }
});

// 處理所有其他路由，並加上 CORS header
router.all('*', () => error(404, 'Not Found'));

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
