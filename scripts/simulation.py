import numpy as np
import pandas as pd
from .calculations import calculate_metrics, EPSILON

def get_rebalancing_dates(df_prices, period):
    if period == 'never': return []
    df = df_prices.copy()
    df['year'] = df.index.year
    df['month'] = df.index.month
    if period == 'annually':
        rebalance_dates = df.drop_duplicates(subset=['year'], keep='first').index
    elif period == 'quarterly':
        df['quarter'] = df.index.quarter
        rebalance_dates = df.drop_duplicates(subset=['year', 'quarter'], keep='first').index
    elif period == 'monthly':
        rebalance_dates = df.drop_duplicates(subset=['year', 'month'], keep='first').index
    else:
        return []
    return rebalance_dates[1:] if len(rebalance_dates) > 1 else []

def run_simulation(portfolio_config, price_data, initial_amount, benchmark_history=None):
    tickers = portfolio_config['tickers']
    weights = np.array(portfolio_config['weights']) / 100.0
    rebalancing_period = portfolio_config['rebalancingPeriod']
    df_prices = price_data[tickers].copy()
    if df_prices.empty: return None
    
    portfolio_history = pd.Series(index=df_prices.index, dtype=float, name="value")
    rebalancing_dates = get_rebalancing_dates(df_prices, rebalancing_period)
    
    current_date = df_prices.index[0]
    initial_prices = df_prices.loc[current_date]
    shares = (initial_amount * weights) / (initial_prices + EPSILON)
    portfolio_history.loc[current_date] = initial_amount
    
    for i in range(1, len(df_prices)):
        current_date = df_prices.index[i]
        current_prices = df_prices.loc[current_date]
        
        current_value = (shares * current_prices).sum()
        portfolio_history.loc[current_date] = current_value
        
        if current_date in rebalancing_dates:
            shares = (current_value * weights) / (current_prices + EPSILON)
            
    portfolio_history.dropna(inplace=True)
    metrics = calculate_metrics(portfolio_history.to_frame('value'), benchmark_history)
    
    return {
        'name': portfolio_config['name'], 
        **metrics, 
        'portfolioHistory': [{'date': date.strftime('%Y-%m-%d'), 'value': value} for date, value in portfolio_history.items()]
    }
