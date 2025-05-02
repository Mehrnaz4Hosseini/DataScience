# src/batch_processing/user_activity_analysis.py
import pymongo
from pymongo import MongoClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from configs import MONGO_URI, MONGO_DB

class UserActivityAnalyzer:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.collection = self.db['valid_transactions']
    
    def get_user_activity_metrics(self):
        # Transactions per user
        pipeline1 = [
            {"$group": {
                "_id": "$customer_id",
                "transaction_count": {"$sum": 1},
                "customer_type": {"$first": "$customer_type"}
            }},
            {"$sort": {"transaction_count": -1}},
            {"$limit": 20}
        ]
        
        # Frequency of activity (transactions per day)
        pipeline2 = [
            {"$group": {
                "_id": {
                    "customer_id": "$customer_id",
                    "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}}
                },
                "daily_count": {"$sum": 1}
            }},
            {"$group": {
                "_id": "$_id.customer_id",
                "avg_daily_transactions": {"$avg": "$daily_count"}
            }},
            {"$sort": {"avg_daily_transactions": -1}},
            {"$limit": 20}
        ]
        
        # Hourly growth trends (modified from daily)
        pipeline3 = [
            {"$group": {
                "_id": {
                    "hour": {"$dateToString": {"format": "%Y-%m-%d %H:00", "date": "$timestamp"}},
                    "customer_type": "$customer_type"
                },
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id.hour": 1}}
        ]
        
        return {
            "transactions_per_user": list(self.collection.aggregate(pipeline1)),
            "activity_frequency": list(self.collection.aggregate(pipeline2)),
            "growth_trends": list(self.collection.aggregate(pipeline3))
        }
    
    def visualize_user_activity(self):
        metrics = self.get_user_activity_metrics()
        
        # Define colors for all customer types
        customer_colors = {
            'individual': 'rgb(26, 118, 255)',
            'business': 'rgb(55, 83, 109)',
            'CIP': 'rgb(255, 127, 14)'
        }
        
        # Create subplots
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=(
                'Top 20 Users by Transaction Count',
                'Top 20 Users by Average Daily Transactions',
                'Hourly Transaction Trends by Customer Type'
            ),
            vertical_spacing=0.1
        )
        
        # 1. Transactions per user
        users1 = [m['_id'] for m in metrics['transactions_per_user']]
        counts1 = [m['transaction_count'] for m in metrics['transactions_per_user']]
        types1 = [m['customer_type'].strip() if m['customer_type'] else 'individual' for m in metrics['transactions_per_user']]
        
        fig.add_trace(
            go.Bar(
                x=users1,
                y=counts1,
                marker_color=[customer_colors.get(t, 'rgb(200, 200, 200)') for t in types1],
                name='Transaction Count',
                text=counts1,
                hovertext=types1
            ),
            row=1, col=1
        )
        
        # 2. Activity frequency
        users2 = [m['_id'] for m in metrics['activity_frequency']]
        freqs = [m['avg_daily_transactions'] for m in metrics['activity_frequency']]
        
        fig.add_trace(
            go.Bar(
                x=users2,
                y=freqs,
                name='Avg Daily Transactions',
                marker_color='rgb(26, 118, 255)',
                text=[f"{f:.1f}" for f in freqs]
            ),
            row=2, col=1
        )
        
        # 3. Hourly growth trends
        hours = sorted(list(set(m['_id']['hour'] for m in metrics['growth_trends'])))
        customer_types = ['individual', 'business', 'CIP']
        
        for customer_type in customer_types:
            counts = []
            for hour in hours:
                record = next((m for m in metrics['growth_trends'] 
                            if m['_id']['hour'] == hour and 
                            m['_id']['customer_type'].strip() == customer_type), None)
                counts.append(record['count'] if record else 0)
            
            fig.add_trace(
                go.Scatter(
                    x=hours,
                    y=counts,
                    mode='lines+markers',
                    name=f'{customer_type.capitalize()} Customers',
                    line=dict(color=customer_colors.get(customer_type, 'rgb(200, 200, 200)')),
                    hoverinfo='x+y+name'
                ),
                row=3, col=1
            )
        
        # Update layout with Y-axis labels
        fig.update_layout(
            height=1200,
            showlegend=True,
            title_text="User Activity Analysis",
            hovermode='x unified'
        )
        
        # Add Y-axis titles for each subplot
        fig.update_yaxes(title_text="Number of Transactions", row=1, col=1)
        fig.update_yaxes(title_text="Number of Transactions /Day", row=2, col=1)
        fig.update_yaxes(title_text="Number of Transactions", row=3, col=1)
        
        # Update x-axis for hourly chart
        fig.update_xaxes(
            tickangle=45,
            tickformat="%Y-%m-%d %H:%M",
            row=3, col=1
        )
        
        fig.show()

if __name__ == "__main__":
    analyzer = UserActivityAnalyzer()
    analyzer.visualize_user_activity()