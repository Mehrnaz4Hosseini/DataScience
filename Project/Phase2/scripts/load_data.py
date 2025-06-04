import pandas as pd
from database_connection import create_connection  # Import connection function

# List of table names to load
TABLES = ["movies", "movie_genres", "movie_casts", "movie_directors", "movie_keywords", "movie_reviews"]

def load_all_tables():
    """Load all tables into separate Pandas DataFrames."""
    conn = create_connection()  # Establish database connection
    
    if conn:
        dataframes = {}  # Dictionary to store DataFrames
        
        for table in TABLES:
            query = f"SELECT * FROM {table}"
            dataframes[table] = pd.read_sql_query(query, conn)  # Load into DataFrame
            
        conn.close()  # Close database connection
        return dataframes
    else:
        print("Failed to connect to database.")
        return None

# Example Execution
if __name__ == "__main__":
    df_dict = load_all_tables()
    
    if df_dict:
        for table_name, df in df_dict.items():
            print(f"✅ Loaded {table_name}: {df.shape[0]} rows")