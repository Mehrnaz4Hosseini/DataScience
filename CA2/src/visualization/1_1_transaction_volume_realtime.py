from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
from confluent_kafka import Consumer
import json
from datetime import datetime, timedelta
from threading import Thread, Lock
from collections import deque
from plotly.subplots import make_subplots
from configs.kafka_config import KAFKA_BOOTSTRAP_SERVERS

# Shared data storage with thread lock
transaction_data = deque(maxlen=1000)
data_lock = Lock()

# Kafka consumer thread
def kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'visualization_group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe(['darooghe.valid_transactions'])
    
    print("Kafka consumer started. Waiting for messages...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            try:
                transaction = json.loads(msg.value().decode('utf-8'))
                with data_lock:
                    transaction_data.append({
                        'timestamp': datetime.fromisoformat(transaction['timestamp']),
                        'amount': float(transaction['amount'])
                    })
            except Exception as e:
                print(f"Error processing message: {e}")
    finally:
        consumer.close()

# Start Kafka consumer in background
Thread(target=kafka_consumer, daemon=True).start()

# Dash app
app = Dash(__name__)

app.layout = html.Div([
    dcc.Graph(id='live-graph'),
    dcc.Interval(id='graph-update', interval=1000, n_intervals=0),
    html.Div(id='debug-output', style={'whiteSpace': 'pre-line'})
])

@app.callback(
    [Output('live-graph', 'figure'),
     Output('debug-output', 'children')],
    [Input('graph-update', 'n_intervals')]
)
def update_graph(n):
    with data_lock:
        current_data = list(transaction_data)  # Create a thread-safe copy
    
    if not current_data:
        debug_msg = "No data received yet. Waiting for Kafka messages..."
        return go.Figure(), debug_msg
    
    try:
        df = pd.DataFrame(current_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        if df.empty:
            return go.Figure(), "DataFrame created but empty"
        
        time_threshold = pd.Timestamp.utcnow().replace(tzinfo=None) - timedelta(seconds=30)
        df_top = df[df.index >= time_threshold]
        
        df_resampled = df.resample('1T').count()
        cutoff_time = pd.Timestamp.utcnow().replace(tzinfo=None) - timedelta(minutes=10)
        df_resampled = df_resampled[df_resampled.index >= cutoff_time]

        
        fig = make_subplots(rows=2, cols=1, subplot_titles=(
            f'Last 30 Seconds Transactions (Real-time)',
            'Transactions per Minute (Aggregated)'
        ))
        
        fig.add_trace(
            go.Scatter(
                x=df_top.index,
                y=df_top['amount'],
                mode='lines+markers',
                name='Real-time Transactions'
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Bar(
                x=df_resampled.index,
                y=df_resampled['amount'],
                name='Transactions per Minute'
            ),
            row=2, col=1
        )
        
        # Update layout for both subplots
        fig.update_layout(
            height=800,
            showlegend=False
        )
        
        # Update y-axis titles for each subplot
        fig.update_yaxes(title_text="Transaction Amount", row=1, col=1)
        fig.update_yaxes(title_text="Transaction Count", row=2, col=1)
        
        # Update x-axis title for bottom subplot only
        fig.update_xaxes(title_text="Time", row=2, col=1)
        
        debug_msg = f"Data points: {len(current_data)}\n"
        debug_msg += f"Displaying last {len(df_top)} transactions in top graph\n"
        debug_msg += f"Time range: {df.index.min()} to {df.index.max()}\n"
        debug_msg += f"Amount range: {df['amount'].min()} to {df['amount'].max()}"
        
        return fig, debug_msg
    
    except Exception as e:
        error_msg = f"Error in visualization: {str(e)}"
        print(error_msg)
        return go.Figure(), error_msg

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)  # Disable reloader to avoid duplicate threads