from sklearn.preprocessing import StandardScaler
import pandas as pd
from load_data import load_all_tables

# Define standardized table names (used for reference or validation)
TABLES = ["movies", "movie_genres", "movie_casts", "movie_directors", "movie_keywords", "movie_reviews"]

def preprocess_data():
    # Load all tables from the database into a dictionary of DataFrames
    df_dict = load_all_tables()

    # Extract each individual table from the dictionary
    if df_dict:
        movies = df_dict.get("movies")

    # Initialize final DataFrame for processed features
    final_df = pd.DataFrame()

    # Retain essential columns for modeling
    final_df['movie_id'] = movies['movie_id']
    final_df['movie_name'] = movies['movie_name']
    final_df['rating'] = movies['rating']
    final_df['certification'] = movies['certification']

    # Normalize the 'rating' column and fill nan values.
    scaler = StandardScaler()
    final_df[['rating']] = scaler.fit_transform(final_df[['rating']])
    final_df['rating'] = final_df['rating'].fillna(final_df['rating'].mean())

    # Map various international certification labels to a unified scale
    cert_map = {
        # General Audience (Suitable for all ages)
        'G': 'G', 'U': 'G', 'ALL': 'G', 'AL': 'G', 'Genel İzleyici Kitlesi': 'G',

        # Parental Guidance Recommended (~7+)
        'PG': 'PG', 'PG-12': 'PG', 'PG-13': 'PG', 'PG13': 'PG', 'PG12': 'PG', 
        '6': 'PG', '6+': 'PG', '7': 'PG', '8': 'PG', 'T': 'PG', 'UA': 'PG',

        # Teen Audiences (Mild to Moderate content, ~12-16)
        '12': '12', '12A': '12', '12+': '12', '12PG': '12', 'K12': '12', 
        'M/12': '12', 'NC16': '12', 'PG-12': '12',

        # Older Teens / Restricted (~14-16)
        '14': '15', '14+': '15', '14A': '15', '15': '15', '15A': '15', 
        '15+': '15', 'M/14': '15', 'R': '15', 'R15+': '15', 'R-15': '15', 
        'B-15': '15', 'MA15+': '15', 'MA 15+': '15', 'K-16': '15', 
        'KT': '15', 'K-15': '15', '15세 이상 관람가': '15', 'NC16': '15',

        # Mature / Restricted (~16-18)
        '16': '18', '16+': '18', 'M/16': '18', 'M18': '18', 'VM18': '18', 
        'R18': '18', 'R 18+': '18', 'R-18': '18', 'NC-17': '18', 'III': '18', 
        'N-18': '18', '청소년 관람불가': '18', '19': '18', '21+': '18',

        # Adults Only (18+)
        '18': '18', '18+': '18', '18A': '18', 'IIB': '18', 'C': '18', 'ITA': '18',

        # Special / Regional Ratings (Uncategorized or unknown)
        'NR': 'unknown', 'TP': 'unknown', 'B': 'unknown', 'B15': 'unknown', 
        'KN': 'unknown', 'S': 'unknown', 'Κ': 'unknown', 'II': 'unknown', 
        'IIA': 'unknown', 'AA': 'unknown', 'Btl': 'unknown', 'K-18': 'unknown',
        'K-7': 'unknown', 'Κ-15': 'unknown', 'APTA': 'unknown', None: 'unknown'
    }

    # Apply the standardized certification mapping to the DataFrame
    final_df['certification'] = movies['certification'].map(cert_map).fillna('unknown')

    # Encode the mapped certification labels as ordinal integers
    ordinal_map = {'unknown': -1, 'G': 0, 'PG': 1, '12': 2, '15': 3, '18': 4}
    final_df['certification'] = final_df['certification'].map(ordinal_map)

    return final_df, df_dict

# Example execution for testing preprocessing pipeline
if __name__ == "__main__":
    final_df, df_dict = preprocess_data()
