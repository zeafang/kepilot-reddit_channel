import os, time
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import praw
from dotenv import load_dotenv

# ---------- Setup ----------
BASE = Path(__file__).resolve().parent
OUT_DIR = BASE / "reddit_metrics"
OUT_DIR.mkdir(exist_ok=True)
OUT_CSV = OUT_DIR / "post_metrics_timeseries.csv"

load_dotenv()  # loads .env

reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    username=os.getenv("REDDIT_USERNAME"),
    password=os.getenv("REDDIT_PASSWORD"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)

# TRACK EITHER specific URLs or your latest posts
POST_URLS = [
    # "https://www.reddit.com/r/Python/comments/xxxxxx/example_post/",
]
TRACK_MY_LATEST_LIMIT = 50  # used if POST_URLS is empty

def row_from_submission(s, run_ts):
    return {
        "run_ts_utc": run_ts,
        "post_id": s.id,
        "permalink": "https://www.reddit.com" + s.permalink,
        "title": s.title,
        "subreddit": s.subreddit.display_name,
        "author": str(s.author) if s.author else None,
        "created_utc": datetime.fromtimestamp(s.created_utc, tz=timezone.utc).isoformat(),
        "score": s.score,
        "upvote_ratio": s.upvote_ratio,
        "num_comments": s.num_comments,
        "over_18": s.over_18,
        "spoiler": s.spoiler,
        "is_self": s.is_self,
    }

def fetch_rows():
    run_ts = datetime.now(timezone.utc).isoformat()
    rows = []
    if POST_URLS:
        for url in POST_URLS:
            s = reddit.submission(url=url)
            s = reddit.submission(id=s.id)  # re-fetch submission
            rows.append(row_from_submission(s, run_ts))
            time.sleep(1)  # polite pacing
    else:
        me = reddit.redditor(os.getenv("REDDIT_USERNAME"))
        for s in me.submissions.new(limit=TRACK_MY_LATEST_LIMIT):
            s = reddit.submission(id=s.id)  # re-fetch submission
            rows.append(row_from_submission(s, run_ts))
            time.sleep(0.5)
    return pd.DataFrame(rows)

def append_csv(df):
    if df.empty:
        print("No rows to write.")
        return
    if OUT_CSV.exists():
        df.to_csv(OUT_CSV, mode="a", header=False, index=False)
    else:
        df.to_csv(OUT_CSV, index=False)
    print(f"Saved {len(df)} rows → {OUT_CSV}")

def compute_growth():
    if not OUT_CSV.exists():
        print("No prior data; growth metrics will be available after the next run.")
        return
    df = pd.read_csv(OUT_CSV)
    # sort by post_id then run_ts
    df["run_ts_utc"] = pd.to_datetime(df["run_ts_utc"], utc=True, errors="coerce")
    df = df.sort_values(["post_id", "run_ts_utc"])
    # per-post lag
    df["score_prev"] = df.groupby("post_id")["score"].shift(1)
    df["num_comments_prev"] = df.groupby("post_id")["num_comments"].shift(1)
    df["dt_hours"] = (
        df.groupby("post_id")["run_ts_utc"].diff().dt.total_seconds() / 3600.0
    )
    df["d_score"] = df["score"] - df["score_prev"]
    df["d_comments"] = df["num_comments"] - df["num_comments_prev"]
    # safe rates per hour
    df["score_per_hr"] = (df["d_score"] / df["dt_hours"]).where(df["dt_hours"] > 0)
    df["comments_per_hr"] = (df["d_comments"] / df["dt_hours"]).where(df["dt_hours"] > 0)
    # latest snapshot per post with growth columns
    latest = df.sort_values("run_ts_utc").groupby("post_id").tail(1)
    out = OUT_DIR / "post_metrics_latest_with_growth.csv"
    latest.to_csv(out, index=False)
    print(f"Computed growth; wrote → {out}")

if __name__ == "__main__":
    data = fetch_rows()
    append_csv(data)
    compute_growth()

