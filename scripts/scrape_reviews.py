from google_play_scraper import Sort, reviews
import pandas as pd
import os

apps = {
    "CBE": "com.combanketh.mobilebanking",
    "BOA": "com.boa.boaMobileBanking",
    "Dashen": "com.dashen.dashensuperapp"
}

def scrape_reviews(app_name, app_id, count=400):
    result, _ = reviews(
        app_id,
        lang='en',
        country='us',
        sort=Sort.NEWEST,
        count=count,
        filter_score_with=None
    )
    data = [{
        "review": r['content'],
        "rating": r['score'],
        "date": r['at'].strftime("%Y-%m-%d"),
        "bank": app_name,
        "source": "Google Play"
    } for r in result]
    return data

all_reviews = []

for bank, app_id in apps.items():
    print(f"Scraping {bank}...")
    all_reviews.extend(scrape_reviews(bank, app_id, count=400))

df = pd.DataFrame(all_reviews)
os.makedirs("data", exist_ok=True)
df.to_csv("data/raw_reviews.csv", index=False)



