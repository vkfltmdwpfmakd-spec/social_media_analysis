import os
import praw
import json
from kafka import KafkaProducer
import logging
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import settings

# Logging 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Kafka Producer 설정 (settings.py에서 가져오기)
try:
    producer = KafkaProducer(
        bootstrap_servers=[settings.KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("Kafka Producer가 성공적으로 연결되었습니다.")
except Exception as e:
    logging.error(f"Kafka Producer 연결 실패: {e}")
    exit()

# Reddit API 설정
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USERNAME = os.getenv('REDDIT_USERNAME')
REDDIT_PASSWORD = os.getenv('REDDIT_PASSWORD')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')

if not all([REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD, REDDIT_USER_AGENT]):
    logging.error("Reddit API 환경변수가 모두 설정되지 않았습니다.")
    exit()

reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    username=REDDIT_USERNAME,
    password=REDDIT_PASSWORD,
    user_agent=REDDIT_USER_AGENT
)

def collect_reddit_data():
    """지정된 서브레딧에서 데이터를 수집하여 Kafka 토픽으로 전송합니다."""
    subreddit_name = settings.SUBREDDIT_NAME
    logging.info(f"Reddit 서브레딧 '{subreddit_name}'에서 데이터를 수집합니다...")
    subreddit = reddit.subreddit(subreddit_name)
    
    try:
        # 게시글 스트림
        for submission in subreddit.stream.submissions(skip_existing=True):
            data = {
                'id': submission.id,
                'title': submission.title,
                'text': submission.selftext,
                'created_utc': submission.created_utc,
                'author_name': str(submission.author),
                'type': 'submission',
                'submission_id': submission.id,
                'parent_id': None
            }
            if data['text'] and data['text'].strip():  # 빈 텍스트 스킵
                producer.send(settings.KAFKA_TOPIC, value=data)
            logging.info(f"수집된 게시글: {data['title'][:50]}...")
            time.sleep(1)

        # 댓글 스트림
        for comment in subreddit.stream.comments(skip_existing=True):
            data = {
                'id': comment.id,
                'title': None,
                'text': comment.body,
                'created_utc': comment.created_utc,
                'author_name': str(comment.author),
                'type': 'comment',
                'submission_id': comment.submission.id,
                'parent_id': comment.parent_id
            }
            if data['text'] and data['text'].strip():  # 빈 텍스트 스킵
                producer.send(settings.KAFKA_TOPIC, value=data)
            logging.info(f"수집된 댓글: {data['text'][:50]}...")
            time.sleep(1)

    except Exception as e:
        logging.error(f"Reddit 데이터 수집 중 오류 발생: {e}", exc_info=True)
        time.sleep(60)

if __name__ == "__main__":
    collect_reddit_data()
