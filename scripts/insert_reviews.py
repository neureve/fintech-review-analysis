import pandas as pd
import oracledb

# Connect to Oracle
connection = oracledb.connect(
    user="bank_reviews",
    password="kifiya123",
    dsn="localhost/XEPDB1"
)
cursor = connection.cursor()

# Read the cleaned data
df = pd.read_csv("data/sentiment_scored_reviews.csv")

# Insert unique banks first
banks = df["bank"].unique()
bank_id_map = {}
for bank in banks:
    cursor.execute("INSERT INTO banks (name) VALUES (:1)", [bank])
    connection.commit()
    cursor.execute("SELECT bank_id FROM banks WHERE name = :1", [bank])
    bank_id_map[bank] = cursor.fetchone()[0]

# Insert reviews
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO reviews (review_text, rating, review_date, source, bank_id, sentiment, sentiment_score)
        VALUES (:1, :2, TO_DATE(:3, 'YYYY-MM-DD'), :4, :5, :6, :7)
    """, [
        row["review"], row["rating"], row["date"],
        row["source"], bank_id_map[row["bank"]],
        row["sentiment"], row["sentiment_score"]
    ])

connection.commit()
cursor.close()
connection.close()

print("✅ Data inserted successfully.")

