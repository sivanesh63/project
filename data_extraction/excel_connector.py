import pandas as pd
import os
import kaggle
from datetime import datetime
from utils.logger import log_pipeline_step, log_data_quality_check
from config import DATA_DIR, EXCEL_FILE_PATH, KAGGLE_DATASET_NAME

class ExcelConnector:
    
    def __init__(self):
        self.data_dir = DATA_DIR
        self.excel_file_path = EXCEL_FILE_PATH
    def download_dataset(self):
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            kaggle.api.dataset_download_files(
                KAGGLE_DATASET_NAME, 
                path=self.data_dir, 
                unzip=True
            )
            return True
        except Exception as e:
            return False
    
    def load_orders_data(self):
        try:
            orders_df = pd.read_excel(self.excel_file_path, sheet_name='Orders')
            self._validate_orders_data(orders_df)
            orders_df = self._transform_orders_data(orders_df)
            return orders_df
        except Exception as e:
            return None
    
    def load_returns_data(self):
        try:
            returns_df = pd.read_excel(self.excel_file_path, sheet_name='Returns')
            self._validate_returns_data(returns_df)
            returns_df = self._transform_returns_data(returns_df)
            return returns_df
        except Exception as e:
            return None
    
    def load_people_data(self):
        try:
            people_df = pd.read_excel(self.excel_file_path, sheet_name='People')
            self._validate_people_data(people_df)
            people_df = self._transform_people_data(people_df)
            return people_df
        except Exception as e:
            return None
    
    def _validate_orders_data(self, df):
        missing_pct = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
        if missing_pct > 0.05:  # 5% threshold
            log_data_quality_check("Orders Missing Data", "WARNING", f"Missing data: {missing_pct:.2%}")
        else:
            log_data_quality_check("Orders Missing Data", "PASS", f"Missing data: {missing_pct:.2%}")
        required_columns = ['Order ID', 'Order Date', 'Ship Date', 'Customer ID', 'Product ID']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            log_data_quality_check("Orders Required Columns", "FAIL", f"Missing columns: {missing_columns}")
        else:
            log_data_quality_check("Orders Required Columns", "PASS")
    
    def _validate_returns_data(self, df):
        missing_pct = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
        if missing_pct > 0.05:
            log_data_quality_check("Returns Missing Data", "WARNING", f"Missing data: {missing_pct:.2%}")
        else:
            log_data_quality_check("Returns Missing Data", "PASS", f"Missing data: {missing_pct:.2%}")
    
    def _validate_people_data(self, df):
        missing_pct = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
        if missing_pct > 0.05:
            log_data_quality_check("People Missing Data", "WARNING", f"Missing data: {missing_pct:.2%}")
        else:
            log_data_quality_check("People Missing Data", "PASS", f"Missing data: {missing_pct:.2%}")
    
    def _transform_orders_data(self, df):
        date_columns = ['Order Date', 'Ship Date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        if 'Order Date' in df.columns and 'Ship Date' in df.columns:
            df['Lead Time (Days)'] = (df['Ship Date'] - df['Order Date']).dt.days
        
        if 'Order Date' in df.columns:
            df['Order Year'] = df['Order Date'].dt.year
            df['Order Month'] = df['Order Date'].dt.month
            df['Order Quarter'] = df['Order Date'].dt.quarter
        
        if 'Sales' in df.columns and 'Quantity' in df.columns:
            df['Order Value'] = df['Sales'] * df['Quantity']
        
        return df
    
    def _transform_returns_data(self, df):
        if 'Return Date' in df.columns:
            df['Return Date'] = pd.to_datetime(df['Return Date'])
        return df
    
    def _transform_people_data(self, df):
        if 'Person' not in df.columns:
            df['Person'] = 'Unknown'
        return df
    
    def get_all_data(self):
        data = {}        
        data['orders'] = self.load_orders_data()
        data['returns'] = self.load_returns_data()
        data['people'] = self.load_people_data()
        return data 