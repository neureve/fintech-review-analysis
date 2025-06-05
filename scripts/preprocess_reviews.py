import pandas as pd

df = pd.read_csv("data/raw_reviews.csv")

# Clean data
df.drop_duplicates(subset=["review", "bank"], inplace=True)
df.dropna(subset=["review", "rating", "date"], inplace=True)
df['date'] = pd.to_datetime(df['date']).dt.date

df.to_csv("data/cleaned_reviews.csv", index=False)
print("✅ Preprocessing complete.")
