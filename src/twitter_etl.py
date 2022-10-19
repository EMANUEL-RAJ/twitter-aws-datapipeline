import io
import tweepy
import pandas as pd
from math import ceil

PER_PAGE_COUNT = 100

def ingest(session):
    """
    Fetches tweets using the 'search' parameter and writes them into S3 bucket.

    :param session: TwitterSession
    """
    tweets_list = []
    page_count = ceil(session.parsed_args.count / PER_PAGE_COUNT)
    print("Getting tweets using search word: {}".format(session.parsed_args.search))
    for page in tweepy.Cursor(session.twitter_api.search_tweets,
                              session.parsed_args.search,
                              count=session.parsed_args.count,
                              tweet_mode='extended').pages(page_count):
        for tweet in page:
            extracted_data = {'id': tweet.id_str,
                              'created_at': tweet.created_at,
                              'text': tweet.full_text,
                              'hashtags': tweet.entities['hashtags'],
                              'symbols': tweet.entities['symbols'],
                              'user_mentions': tweet.entities['user_mentions'],
                              'urls': tweet.entities['urls'],
                              'result_type': tweet.metadata['result_type'],
                              'language_code': tweet.metadata['iso_language_code'],
                              'in_reply_to_status_id_str': tweet.in_reply_to_status_id_str,
                              'in_reply_to_user_id_str': tweet.in_reply_to_user_id_str,
                              'in_reply_to_screen_name': tweet.in_reply_to_screen_name,
                              'geo': tweet.geo,
                              'retweet_count': tweet.retweet_count,
                              'favorite_count': tweet.favorite_count,
                              'lang': tweet.lang,
                              'author_id': tweet.author.id_str,
                              'author_screen_name': tweet.author.screen_name,
                              'author_name': tweet.author.name,
                              'author_location': tweet.author.location,
                              'author_profile_desc': tweet.author.description}
            tweets_list.append(extracted_data)
    data = pd.DataFrame(tweets_list)
    print("Extracted {} tweets.".format(len(data.index)))
    s3_upload(session, data)


def s3_upload(session, data):
    try:
        print("Uploading data into S3 bucket - {}".format(session.aws_bucket_name))
        with io.StringIO() as csv_buffer:
            data.to_csv(csv_buffer, index=False)

            response = session.s3_client.put_object(Bucket=session.aws_bucket_name,
                                                    Key='landing/{}'.format(session.file_name),
                                                    Body=csv_buffer.getvalue()
                                                    )
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print("File {} uploaded successfully".format(session.file_name))
            else:
                print("Upload failed. Status: {}".format(status))
    except Exception as e: 
        print("Pipeline failed. Error: {}".format(e))
