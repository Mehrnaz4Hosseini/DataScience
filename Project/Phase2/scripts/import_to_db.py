import pandas as pd
import numpy as np
import re
from dateutil import parser
import ast
import json
import sqlite3
import os

script_dir = os.path.dirname(os.path.abspath(__file__))  # The directory of the script
data_path = os.path.join(script_dir, "..", "data", "movies_data.csv")
df = pd.read_csv(data_path)

# Converts runtime strings like '2h 30m' to total minutes
def parse_runtime(runtime_str):
    if pd.isna(runtime_str): return np.nan
    match = re.findall(r'(\d+)h|(\d+)m', runtime_str)
    minutes = 0
    for h, m in match:
        if h:
            minutes += int(h) * 60
        if m:
            minutes += int(m)
    return minutes

# Attempts to convert various date formats to datetime objects
def convert_to_date(date_str):
    try:
        return pd.to_datetime(date_str, format='%d-%b-%y')  
    except:
        try:
            return parser.parse(date_str, dayfirst=True)
        except:
            return pd.NaT

# Safely converts a string representation of a list/dict into a Python object
def convert_to_literal(value):
    try:
        return ast.literal_eval(value)
    except Exception as e:
        return []

# Parses the JSON-style 'reviews' field into usable format
def parse_reviews(raw):
    try:
        if isinstance(raw, str):
            return ast.literal_eval(raw)
        else:
            return []
    except Exception as e:
        return []

# ---------------------- Preprocessing Function ----------------------

# Cleans and transforms the raw dataset into structured tables
def preprocessing_before_import_to_db(df):

    # --- Column Cleaning and Type Conversion ---
    df['run_time'] = df['run_time'].apply(parse_runtime)

    # Budget and revenue cleaning: strip symbols and convert to float
    df['budget'].replace('-', np.nan, inplace=True)
    df['budget'] = df['budget'].str.replace('$', '', regex=False)
    df['budget'] = df['budget'].str.replace(',', '').astype(float)

    df['revenue'].replace('-', np.nan, inplace=True)
    df['revenue'] = df['revenue'].str.replace('$', '', regex=False)
    df['revenue'] = df['revenue'].str.replace(',', '').astype(float)

    # Replace empty lists in keywords/reviews with NaN
    df['normal_keyword_(rounded)'].replace('[]', np.nan, inplace=True)
    df['tone_keyword_(bold)'].replace('[]', np.nan, inplace=True)
    df['reviews'].replace('[]', np.nan, inplace=True)

    df['certification'].replace('-', np.nan, inplace=True)

    # Convert release dates and handle future-dated errors
    df['release_date'] = df['release_date'].apply(convert_to_date)
    df.loc[df['release_date'] > pd.Timestamp.today(), 'release_date'] -= pd.DateOffset(years=100)

    # --- Create Movies Table ---
    movies_df = df[[ 'movie_name', 'release_date', 'rating', 'run_time',
        'certification', 'overview', 'tagline', 'language', 'budget',
        'revenue', 'content_score', 'content_score_description']].copy()
    movies_df['movie_id'] = movies_df.index
    columns = movies_df.columns.tolist()
    movies_df = movies_df[[columns[-1]] + columns[:-1]]

    # --- Genre Table ---
    movie_genres = df[['genre']].copy()
    movie_genres['movie_id'] = df.index
    movie_genres['genre'] = movie_genres['genre'].apply(convert_to_literal)
    movie_genres = movie_genres.explode('genre')
    columns = movie_genres.columns.tolist()
    movie_genres = movie_genres[[columns[-1]] + columns[:-1]]

    # --- Cast Table ---
    movie_cast = df[['cast']].copy()
    movie_cast['movie_id'] = df.index
    movie_cast['cast'] = movie_cast['cast'].apply(convert_to_literal)
    movie_cast = movie_cast.explode('cast')
    columns = movie_cast.columns.tolist()
    movie_cast = movie_cast[[columns[-1]] + columns[:-1]]

    # --- Directors Table ---
    movie_directors = df[['director']].copy()
    movie_directors['movie_id'] = movie_directors.index
    movie_directors['director'] = movie_directors['director'].apply(convert_to_literal)
    movie_directors = movie_directors.explode('director')
    columns = movie_directors.columns.tolist()
    movie_directors = movie_directors[[columns[-1]] + columns[:-1]]

    # --- Keywords Table ---
    normal_kw = df[['normal_keyword_(rounded)']].copy()
    normal_kw['movie_id'] = normal_kw.index
    normal_kw['normal_keyword_(rounded)'] = normal_kw['normal_keyword_(rounded)'].apply(convert_to_literal)
    normal_kw = normal_kw.explode('normal_keyword_(rounded)')

    tone_kw = df[['tone_keyword_(bold)']].copy()
    tone_kw['movie_id'] = tone_kw.index
    tone_kw['tone_keyword_(bold)'] = tone_kw['tone_keyword_(bold)'].apply(convert_to_literal)
    tone_kw = tone_kw.explode('tone_keyword_(bold)')

    movie_keywords = normal_kw.merge(tone_kw, on='movie_id', how='outer')
    columns = ['movie_id'] + [col for col in movie_keywords.columns if col != 'movie_id']
    movie_keywords = movie_keywords[columns]

    # --- Reviews Table ---
    review_parsed = pd.DataFrame(df['reviews'].apply(parse_reviews))
    review_parsed['movie_id'] = review_parsed.index
    review_parsed = review_parsed[['movie_id', 'reviews']].explode('reviews')

    movie_reviews = pd.json_normalize(review_parsed['reviews'])
    movie_reviews['movie_id'] = review_parsed['movie_id'].values

    # Review cleanup: normalize scores and genres
    movie_reviews['review'] = movie_reviews['review'].replace('.', np.nan)
    movie_reviews['most_watched_genres'] = movie_reviews['most_watched_genres'].apply(
        lambda x: np.nan if isinstance(x, list) and len(x) == 0 else x)
    movie_reviews['most_watched_genres'] = movie_reviews['most_watched_genres'].apply(json.dumps)
    movie_reviews['score'] = movie_reviews['score'].str.replace('%', '', regex=False)
    movie_reviews['score'] = movie_reviews['score'].replace('N/A', np.nan)
    movie_reviews['score'] = movie_reviews['score'].astype(float)
    columns = movie_reviews.columns.tolist()
    movie_reviews = movie_reviews[[columns[-1]] + columns[:-1]]

    return movies_df, movie_genres, movie_cast, movie_directors, movie_keywords, movie_reviews


# ---------------------- SQLite Table Creation ----------------------

# Creates SQLite tables and imports preprocessed data
def create_tables(movies_df, movie_genres, movie_cast, movie_directors, movie_keywords, movie_reviews):

    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "..", "database", "dataset.db")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Main movies table
    c.execute('''
    CREATE TABLE IF NOT EXISTS movies (
    movie_id INT NOT NULL PRIMARY KEY,
    movie_name VARCHAR(255),
    release_date DATE,
    rating FLOAT,
    run_time_minutes INT,
    certification VARCHAR(20),
    overview TEXT,
    tagline TEXT,
    language VARCHAR(50),
    budget BIGINT,
    revenue BIGINT,
    content_score FLOAT,
    content_score_description TEXT);
          ''')
    movies_df.to_sql('movies', conn, if_exists='replace', index=False)

    # Genre table
    c.execute('''
    CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INT,
    genre VARCHAR(100),
    PRIMARY KEY (movie_id, genre),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id));
          ''')
    movie_genres.to_sql('movie_genres', conn, if_exists='replace', index=False)

    # Cast table
    c.execute('''
    CREATE TABLE IF NOT EXISTS movie_casts (
    movie_id INT,
    actor VARCHAR(100),
    PRIMARY KEY (movie_id, actor),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id));
            ''')
    movie_cast.to_sql('movie_casts', conn, if_exists='replace', index=False)

    # Director table
    c.execute('''
    CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id INT,
    director VARCHAR(100),
    PRIMARY KEY (movie_id, director),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id));
          ''')
    movie_directors.to_sql('movie_directors', conn, if_exists='replace', index=False)

    # Keyword table
    c.execute('''
    CREATE TABLE IF NOT EXISTS movie_keywords (
    movie_id INT,
    normal_keyword_rounded VARCHAR(100),
    tone_keyword_bold VARCHAR(100),
    PRIMARY KEY (normal_keyword_rounded, tone_keyword_bold),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id));
          ''')
    movie_keywords.to_sql('movie_keywords', conn, if_exists='replace', index=False)

    # Reviews table
    c.execute('''
    CREATE TABLE IF NOT EXISTS movie_reviews (
    movie_id INT,
    writer VARCHAR(255),
    score FLOAT,
    review TEXT,
    most_watched_genres TEXT,
    PRIMARY KEY (movie_id, writer),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id));
          ''')
    movie_reviews.to_sql('movie_reviews', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    # Preprocess raw CSV into structured DataFrames
    movies_df, movie_genres, movie_cast, movie_directors, movie_keywords, movie_reviews = preprocessing_before_import_to_db(df)

    # Create tables and load the data into SQLite
    create_tables(movies_df, movie_genres, movie_cast, movie_directors, movie_keywords, movie_reviews)
