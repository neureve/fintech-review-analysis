import pandas as pd
import oracledb

# Load the cleaned reviews DataFrame
df = pd.read_csv('./data/sentiment_reviews.csv')

  # adjust path if needed

# Connect to Oracle
connection = oracledb.connect(
    user="bank_reviews",
    password="bank123",
    dsn="192.168.1.9:1521/xepdb1"
)
cursor = connection.cursor()

# Get unique bank names from the dataframe
bank_names = df['bank'].unique()

bank_id_map = {}

# Step 2: Insert unique banks (avoid duplicates)
for name in bank_names:
    name_cleaned = name.strip().upper()

    # Check if the bank already exists
    cursor.execute("SELECT id FROM banks WHERE UPPER(name) = :1", [name_cleaned])
    result = cursor.fetchone()

    if result:
        bank_id = result[0]
    else:
        id_var = cursor.var(int)
        cursor.execute("INSERT INTO banks (name) VALUES (:1) RETURNING id INTO :2", [name_cleaned, id_var])
        bank_id = id_var.getvalue()[0]

    bank_id_map[name] = bank_id

# Step 3: Insert reviews
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO reviews (bank_id, review_text, rating, sentiment, review_date)
        VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'))
    """, [
        bank_id_map[row['bank']],
        row['review'],
        row.get('rating', None),
        row['sentiment'],
        str(row['date'])[:10]
  # Extract date part
    ])

# Step 4: Commit
connection.commit()
cursor.close()
connection.close()

print("✅ Data inserted successfully.")

