from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import MultiLabelBinarizer, StandardScaler
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import pandas as pd
import os
from preprocess import preprocess_data

def feature_enguneering():
    # Load preprocessed data and metadata
    final_df, df_dict = preprocess_data()

    # Extract individual tables
    movies = df_dict.get("movies")
    movie_genres = df_dict.get("movie_genres")
    movie_casts = df_dict.get("movie_casts")
    movie_directors = df_dict.get("movie_directors")
    movie_keywords = df_dict.get("movie_keywords")
    movie_reviews = df_dict.get("movie_reviews")

    # ---- BUDGET-REVENUE RATIO ----
    # Compute and normalize budget-to-revenue ratio to capture profitability
    final_df['budget_revenue_ratio'] = movies.apply(
        lambda row: row['budget'] / row['revenue'] if row['revenue'] != 0 else 0, axis=1
    )
    # Handle outliers using IQR and clip values to reduce skew
    Q1 = final_df['budget_revenue_ratio'].quantile(0.25)
    Q3 = final_df['budget_revenue_ratio'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    final_df['budget_revenue_ratio'] = np.clip(final_df['budget_revenue_ratio'], lower_bound, upper_bound)

    # Standardize the ratio
    scaler = StandardScaler()
    final_df[['budget_revenue_ratio']] = scaler.fit_transform(final_df[['budget_revenue_ratio']])
    final_df['budget_revenue_ratio'] = final_df['budget_revenue_ratio'].fillna(final_df['budget_revenue_ratio'].mean())

    # ---- TEXT EMBEDDING: OVERVIEW ----
    # Clean movie overview text
    final_df['overview'] = movies['overview'].fillna('').str.strip().str.replace('\n', ' ')

    # Generate dense sentence embeddings using SentenceTransformer
    model = SentenceTransformer('all-MiniLM-L6-v2')
    overview_embeddings = model.encode(movies['overview'].tolist(), show_progress_bar=True)
    final_df['overview'] = list(overview_embeddings)

    # ---- TEXT EMBEDDING: REVIEWS ----
    # Clean and concatenate all reviews per movie
    movie_reviews_clean = movie_reviews[['movie_id', 'review']]
    movie_reviews_clean['review'] = movie_reviews['review'].fillna('').str.strip().str.replace('\n', ' ')
    reviews_grouped = movie_reviews_clean.groupby('movie_id')['review'].apply(lambda x: ' '.join(x)).reset_index()

    # Encode concatenated reviews
    review_embeddings = model.encode(reviews_grouped['review'].tolist(), show_progress_bar=True)
    final_df['review'] = list(review_embeddings)

    # ---- ONE-HOT ENCODING: TOP 100 ACTORS ----
    # Limit to top 100 actors
    top_100_actors = movie_casts['cast'].value_counts().head(100).index.tolist()
    filtered_cast = movie_casts[movie_casts['cast'].isin(top_100_actors)]

    # One-hot encode actors
    one_hot_df = pd.crosstab(filtered_cast['movie_id'], filtered_cast['cast'])
    one_hot_df.columns = ['actor_' + col.replace(' ', '_') for col in one_hot_df.columns]
    final_df = final_df.merge(one_hot_df, how='left', left_on='movie_id', right_index=True)
    final_df[one_hot_df.columns] = final_df[one_hot_df.columns].fillna(0).astype(int)

    # ---- ONE-HOT ENCODING: TOP 100 DIRECTORS ----
    top_100_directors = movie_directors['director'].value_counts().head(100).index.tolist()
    filtered_director = movie_directors[movie_directors['director'].isin(top_100_directors)]

    one_hot_df = pd.crosstab(filtered_director['movie_id'], filtered_director['director'])
    one_hot_df.columns = ['director_' + col.replace(' ', '_') for col in one_hot_df.columns]
    final_df = final_df.merge(one_hot_df, how='left', left_on='movie_id', right_index=True)
    final_df[one_hot_df.columns] = final_df[one_hot_df.columns].fillna(0).astype(int)

    # ---- TF-IDF ENCODING: KEYWORDS ----
    # Combine and clean keyword text
    keywords_combined = movie_keywords.groupby('movie_id').agg({
        'normal_keyword_(rounded)': lambda x: ' '.join(sorted(set(x.dropna().astype(str)))),
        'tone_keyword_(bold)': lambda x: ' '.join(sorted(set(x.dropna().astype(str))))
    }).reset_index()

    # Apply TF-IDF to 'normal_keyword_(rounded)'
    tfidf_vectorizer = TfidfVectorizer(max_features=300)
    tfidf_matrix = tfidf_vectorizer.fit_transform(keywords_combined['normal_keyword_(rounded)'])
    tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=tfidf_vectorizer.get_feature_names_out())
    tfidf_df.index = keywords_combined.index
    exclude_cols = final_df.columns.tolist()
    final_df = pd.concat([final_df, tfidf_df], axis=1)
    final_df.rename(
        columns={col: f"{col}_normal_keyword_(rounded)" for col in final_df.columns if col not in exclude_cols},
        inplace=True
    )

    # Apply TF-IDF to 'tone_keyword_(bold)'
    tfidf_matrix = tfidf_vectorizer.fit_transform(keywords_combined['tone_keyword_(bold)'])
    tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=tfidf_vectorizer.get_feature_names_out())
    tfidf_df.index = keywords_combined.index
    exclude_cols = final_df.columns.tolist()
    final_df = pd.concat([final_df, tfidf_df], axis=1)
    final_df.rename(
        columns={col: f"{col}_tone_keyword_(bold)" for col in final_df.columns if col not in exclude_cols},
        inplace=True
    )

    # ---- ONE-HOT ENCODING: GENRES ----
    # Group genres per movie
    movie_genres_grouped = movie_genres.groupby('movie_id')['genre'].apply(list).reset_index()
    movie_genres_grouped['genre'] = movie_genres_grouped['genre'].apply(
        lambda genres: ['unknown' if pd.isna(g) else g for g in genres]
    )

    # Binarize genre lists
    mlb = MultiLabelBinarizer()
    genre_array = mlb.fit_transform(movie_genres_grouped['genre'])
    genre_df = pd.DataFrame(genre_array, columns=['genre_' + g for g in mlb.classes_])
    genre_df['movie_id'] = movie_genres_grouped['movie_id']

    # Merge genres into final dataframe
    final_df = final_df.merge(genre_df, on='movie_id', how='left')

    output_dir = os.path.join(".", "data")
    os.makedirs(output_dir, exist_ok=True)  # Creates 'data' if it doesn't exist

    final_df.to_csv(os.path.join(output_dir, "finall_df.csv"), index=False)

if __name__ == "__main__":
    feature_enguneering()