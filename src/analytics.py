import pandas as pd

class SalesAnalytics:
    def __init__(self, stg_sales, stg_product, stg_location, stg_payment_method):
        self.sales = stg_sales.copy()
        self.product = stg_product.copy()
        self.location = stg_location.copy()
        self.payment = stg_payment_method.copy()
        
        self.sales['transaction_date'] = pd.to_datetime(self.sales['transaction_date'])
    
    
    #!! maybe remove
    '''
    def summary_metrics(self):
        total_sales = self.sales['total_spent'].sum()
        avg_transaction = self.sales['total_spent'].mean()
        num_transactions = self.sales.shape[0]
        summary = pd.DataFrame({
            'Metric': ['Total Sales', 'Average Transaction', 'Number of Transactions'],
            'Value': [total_sales, avg_transaction, num_transactions]
        })
        summary = summary.sort_values('Value', ascending=False).reset_index(drop=True)
        summary.title = "Overall Sales Summary"
        return summary
    '''
   
    def sales_by_product(self):
        df = self.sales.groupby('product_id')['total_spent'].sum().reset_index()
        df = df.merge(self.product[['product_id', 'Item']], on='product_id')
        df = df[['Item', 'total_spent']].sort_values('total_spent', ascending=False).reset_index(drop=True)
        df.title = "Sales by Product (Greatest to Least)"
        return df
    
    
    def sales_by_location(self):
        df = self.sales.groupby('location_id')['total_spent'].sum().reset_index()
        df = df.merge(self.location[['location_id', 'location_type']], on='location_id')
        df = df[['location_type', 'total_spent']].sort_values('total_spent', ascending=False).reset_index(drop=True)
        df.title = "Sales by Location (Greatest to Least)"
        return df
    
    
    def sales_by_payment(self):
        df = self.sales.groupby('payment_id')['total_spent'].sum().reset_index()
        df = df.merge(self.payment[['payment_id', 'payment_method']], on='payment_id')
        df = df[['payment_method', 'total_spent']].sort_values('total_spent', ascending=False).reset_index(drop=True)
        df.title = "Sales by Payment Method (Greatest to Least)"
        return df
    
   
    def daily_sales(self):
        df = self.sales.groupby(self.sales['transaction_date'].dt.date)['total_spent'].sum().reset_index(name='total_spent')
        df = df.rename(columns={df.columns[0]: 'transaction_date'})
        return df