import sqlite3
import os

def create_connection():
    """Establish connection to SQLite database with proper relative path."""
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get script location
    db_path = os.path.join(script_dir, "..", "database", "dataset.db")  # Step back & go into database

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        print("Connected successfully!")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")

    return conn

if __name__ == "__main__":
    create_connection()