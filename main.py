import pandas as pd


orders = pd.read_excel('train_with_return.xlsx', sheet_name='train')
returns = pd.read_excel('train_with_return.xlsx', sheet_name='Return')
# Sample data exploration
# print(f"Total orders: {len(orders)}")
# print(f"Date range: {orders['Order Date'].min()} to {orders['Order Date'].max()}")
# print(f"Product categories: {orders['Category'].unique()}")
print(orders.head())
print(returns.head())

