import numpy as np
import pandas as pd

# --- 全域常數 ---
RISK_FREE_RATE = 0.0
TRADING_DAYS_PER_YEAR = 252
DAYS_PER_YEAR = 365.25
EPSILON = 1e-9

def calculate_metrics(portfolio_history, benchmark_history=None, risk_free_rate=RISK_FREE_RATE):
    """
    計算績效指標，包含 CAGR, MDD, Volatility, Sharpe, Sortino, Beta, Alpha。
    """
    if portfolio_history.empty or len(portfolio_history) < 2:
        return {'cagr': 0, 'mdd': 0, 'volatility': 0, 'sharpe_ratio': 0, 'sortino_ratio': 0, 'beta': None, 'alpha': None}
    
    end_value = portfolio_history['value'].iloc[-1]
    start_value = portfolio_history['value'].iloc[0]
    if start_value < EPSILON:
        return {'cagr': 0, 'mdd': -1, 'volatility': 0, 'sharpe_ratio': 0, 'sortino_ratio': 0, 'beta': None, 'alpha': None}

    start_date = portfolio_history.index[0]
    end_date = portfolio_history.index[-1]
    years = (end_date - start_date).days / DAYS_PER_YEAR
    cagr = (end_value / start_value) ** (1 / years) - 1 if years > 0 else 0

    portfolio_history['peak'] = portfolio_history['value'].cummax()
    portfolio_history['drawdown'] = (portfolio_history['value'] - portfolio_history['peak']) / (portfolio_history['peak'] + EPSILON)
    mdd = portfolio_history['drawdown'].min()

    daily_returns = portfolio_history['value'].pct_change().dropna()
    if len(daily_returns) < 2:
        return {'cagr': cagr, 'mdd': mdd, 'volatility': 0, 'sharpe_ratio': 0, 'sortino_ratio': 0, 'beta': None, 'alpha': None}

    annual_std = daily_returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    annualized_excess_return = cagr - risk_free_rate
    sharpe_ratio = annualized_excess_return / (annual_std + EPSILON)

    daily_risk_free_rate = (1 + risk_free_rate)**(1/TRADING_DAYS_PER_YEAR) - 1
    downside_returns = daily_returns - daily_risk_free_rate
    downside_returns[downside_returns > 0] = 0
    downside_std = np.sqrt((downside_returns**2).mean()) * np.sqrt(TRADING_DAYS_PER_YEAR)
    
    sortino_ratio = 0.0
    if downside_std > EPSILON:
        sortino_ratio = annualized_excess_return / downside_std

    beta, alpha = None, None
    if benchmark_history is not None and not benchmark_history.empty:
        benchmark_returns = benchmark_history['value'].pct_change().dropna()
        aligned_returns = pd.concat([daily_returns, benchmark_returns], axis=1, join='inner')
        aligned_returns.columns = ['portfolio', 'benchmark']
        if len(aligned_returns) > 1:
            covariance_matrix = aligned_returns.cov()
            covariance = covariance_matrix.iloc[0, 1]
            benchmark_variance = covariance_matrix.iloc[1, 1]
            if benchmark_variance > EPSILON:
                beta = covariance / benchmark_variance
                bench_end_value = benchmark_history['value'].iloc[-1]
                bench_start_value = benchmark_history['value'].iloc[0]
                bench_cagr = (bench_end_value / bench_start_value) ** (1 / years) - 1 if years > 0 else 0
                expected_return = risk_free_rate + beta * (bench_cagr - risk_free_rate)
                alpha = cagr - expected_return

    if not np.isfinite(sharpe_ratio) or np.isnan(sharpe_ratio): sharpe_ratio = 0.0
    if not np.isfinite(sortino_ratio) or np.isnan(sortino_ratio): sortino_ratio = 0.0
    if beta is not None and (not np.isfinite(beta) or np.isnan(beta)): beta = None
    if alpha is not None and (not np.isfinite(alpha) or np.isnan(alpha)): alpha = None

    return {'cagr': cagr, 'mdd': mdd, 'volatility': annual_std, 'sharpe_ratio': sharpe_ratio, 'sortino_ratio': sortino_ratio, 'beta': beta, 'alpha': alpha}
