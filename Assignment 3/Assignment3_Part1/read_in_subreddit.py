import praw
import time
from kafka import KafkaProducer
import json

# Create a Reddit instance
reddit = praw.Reddit(
    client_id='SkvjrKLMp6dhRXyrWTqXBw',
    client_secret='BTn1uJf__AQ0UNi5DgkVTGELAazA6Q',
    user_agent='my_reddit_app:v1.0 (by u/Reality314)'
)

# Kafka producer
kafka_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Get the name of the subreddit we're reading in
subreddit = reddit.subreddit('rupaulsdragrace')

# Read in a stream of submissions from the subreddit
for submission in subreddit.stream.submissions():
    try:
        # Prepare message to send to Kafka topic 'topic1'
        submission_info = {
            "title": submission.title,
            "author": str(submission.author),
            "text": submission.selftext,
            "url": submission.url
        }
        
        # Send message to 'topic1'
        kafka_producer.send('topic1', value=submission_info)
        
        # Print to console so you can see what was sent to 'topic1'
        print(f"Title: {submission.title}")
        print(f"Author: {submission.author}")
        print(f"Text: {submission.selftext}")
        print(f"URL: {submission.url}")
        print("="*80)
        
        time.sleep(1)  # Done to not overwhelm the Reddit API
    except Exception as e:
        print(f"THIS IS THE ERROR: {e}")
        continue
