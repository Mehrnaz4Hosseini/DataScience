import pymongo
from pymongo import MongoClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
from configs import MONGO_URI, MONGO_DB

class HistoricalVolumeAnalyzer:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.collection = self.db['valid_transactions']
    
    def get_historical_data(self, time_range='7d'):
        """Get historical data for specified time range"""
        now = datetime.utcnow()
        
        if time_range == '24h':
            start_time = now - timedelta(hours=24)
            resample_freq = '1H'
        elif time_range == '7d':
            start_time = now - timedelta(days=7)
            resample_freq = '1D'
        elif time_range == '30d':
            start_time = now - timedelta(days=30)
            resample_freq = '1D'
        else: 
            start_time = now - timedelta(days=7)
            resample_freq = '1D'

        query = {'timestamp': {'$gte': start_time, '$lte': now}}
        projection = {'timestamp': 1, 'amount': 1, '_id': 0}
        cursor = self.collection.find(query, projection)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

        df_amount = df['amount'].resample('1H').sum().to_frame('total_amount')
        df_count = df['amount'].resample(resample_freq).count().to_frame('transaction_count')
        
        return df_amount, df_count
    
    def visualize_historical_volume(self, time_range='7d'):
        df_amount, df_count = self.get_historical_data(time_range)
        
        if df_amount.empty:
            print("No historical data found for the selected time range")
            return
        
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=(
                f'Hourly Transaction Amounts (Last {time_range})',
                f'Daily Transaction Counts (Last {time_range})'
            ),
            vertical_spacing=0.15
        )
        
        fig.add_trace(
            go.Scatter(
                x=df_amount.index,
                y=df_amount['total_amount'],
                mode='lines+markers',
                name='Amount',
                line=dict(color='rgb(26, 118, 255)'),
                hovertemplate='%{x|%Y-%m-%d %H:%M}<br>Amount: %{y:,.2f}<extra></extra>'
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Bar(
                x=df_count.index,
                y=df_count['transaction_count'],
                name='Count',
                marker_color='rgb(55, 83, 109)',
                hovertemplate='%{x|%Y-%m-%d}<br>Count: %{y}<extra></extra>'
            ),
            row=2, col=1
        )
        
        fig.update_layout(
            height=800,
            title_text=f'Historical Transaction Analysis (Last {time_range})',
            hovermode='x unified',
            showlegend=False
        )
        
        fig.update_yaxes(title_text="Total Amount", row=1, col=1)
        fig.update_yaxes(title_text="Transaction Count", row=2, col=1)
        fig.update_xaxes(title_text="Time", row=2, col=1)
        
        fig.update_xaxes(
            tickformat="%Y-%m-%d %H:%M",
            row=1, col=1
        )
        fig.update_xaxes(
            tickformat="%Y-%m-%d",
            row=2, col=1
        )
        
        fig.show()

if __name__ == "__main__":
    analyzer = HistoricalVolumeAnalyzer()

    analyzer.visualize_historical_volume(time_range='7d')