import pymongo
from pymongo import MongoClient
import plotly.express as px
from configs import MONGO_URI, MONGO_DB

class MerchantAnalyzer:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.collection = self.db['valid_transactions']
    
    def get_top_merchants(self, limit=5):
        pipeline = [
            {"$group": {
                "_id": "$merchant_id",
                "transaction_count": {"$sum": 1},
                "total_amount": {"$sum": "$amount"},
                "merchant_category": {"$first": "$merchant_category"}
            }},
            {"$sort": {"transaction_count": -1}},
            {"$limit": limit}
        ]
        results = list(self.collection.aggregate(pipeline))
        return results
    
    def visualize_top_merchants(self):
        top_merchants = self.get_top_merchants()

        merchants = [m['_id'] for m in top_merchants]
        counts = [m['transaction_count'] for m in top_merchants]
        categories = [m['merchant_category'] for m in top_merchants]
        
        fig = px.bar(
            x=merchants,
            y=counts,
            color=categories,
            title='Top 5 Merchants by Transaction Count',
            labels={'x': 'Merchant ID', 'y': 'Number of Transactions'},
            text=counts
        )
        
        fig.update_traces(texttemplate='%{text}', textposition='outside')
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
        fig.show()

if __name__ == "__main__":
    analyzer = MerchantAnalyzer()
    analyzer.visualize_top_merchants()