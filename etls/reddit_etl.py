import praw
from praw import Reddit
import sys
from utils.constants import POST_FIELDS
import pandas as pd
import numpy as np

def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        print("Connected to Reddit successfully.")
        return reddit
    except Exception as e:
        print(f"Failed to connect to Reddit: {e}")
        sys.exit(1)
        
def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
    try:
        subreddit_instance = reddit_instance.subreddit(subreddit)
        posts = subreddit_instance.top(time_filter=time_filter, limit=limit)
        post_lists = []
        
        for post in posts:
            post_dict = vars(post)
            post = {key: post_dict[key] for key in POST_FIELDS}
            post_lists.append(post)

        print(post_lists[:5])  # Print first 5 posts for verification
        return post_lists
    except Exception as e:
        print(f"Failed to extract posts: {e}")
        sys.exit(1)
        
def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where(post_df['over_18'] == True, True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['upvote_ratio'] = post_df['upvote_ratio'].astype(int)
    post_df['selftext'] = post_df['selftext'].astype(str)
    post_df['title'] = post_df['title'].astype(str)
    
    return post_df

def load_data_to_csv(post_df: pd.DataFrame, path: str):
    post_df.to_csv(path, index=False)