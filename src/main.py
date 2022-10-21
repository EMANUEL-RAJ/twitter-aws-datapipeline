import os
import sys
import boto3
import tweepy
import logging
from datetime import datetime
from argparse import ArgumentParser
from configparser import ConfigParser
import twitter_etl

NOW = datetime.now().strftime("%Y%d%m%H%M%S")
LOG_FORMAT = '%(asctime)s : %(name)s : %(levelname)s - 	%(message)s'
LOG_FILE_NAME = 'twitter_datapipeline_{}.log'.format(datetime.now().strftime("%Y%d%m%H%M%S"))

# Configuring filehandler for logger
formatter = logging.Formatter(LOG_FORMAT, datefmt="%d/%m/%Y %H:%M:%S")
file_handler = logging.FileHandler('../logs/{}'.format(LOG_FILE_NAME))
file_handler.setFormatter(formatter)

# Configuring logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

class TwitterSession:
    def __init__(self, arguments):
        """
        Creates twitter api and S3 bucket clients using provided credentials and wraps them into a session.

        :parameter argparse.ArgumentParser arguments: Session parameters
        """
        # Setting session parameters
        self.parsed_args = arguments

        if arguments.env == 'local':
            credentials = ConfigParser()
            credentials.read(os.path.join(os.path.expanduser('~'), '.pipeline.conf'))
            self.__aws_access_key = credentials.get('aws', 'aws_access_key_id')
            self.__aws_secret_key = credentials.get('aws', 'aws_secret_access_key')
            self.aws_bucket_name = credentials.get('aws', 'aws_bucket_name')
            self.__consumer_key = credentials.get('twitter', 'twitter_api_key')
            self.__consumer_secret = credentials.get('twitter', 'twitter_key_secret')
            self.__access_token = credentials.get('twitter', 'twitter_access_token')
            self.__access_token_secret = credentials.get('twitter', 'twitter_token_secret')

        self.twitter_api = self.get_api()
        self.s3_client = self.get_s3_client()
        self.file_name = "twitter_search_data_{}.csv".format(NOW)

    def get_api(self):
        """
        Creates tweepy API object using provided credentials.

        :return: tweepy.API
        """
        auth = tweepy.OAuth1UserHandler(consumer_key=self.__consumer_key,
                                        consumer_secret=self.__consumer_secret,
                                        access_token=self.__access_token,
                                        access_token_secret=self.__access_token_secret
                                        )
        api = tweepy.API(auth)
        logger.info("Created twitter api client")
        return api

    def get_s3_client(self):
        """
        Creates s3 client object to write data.
        
        :return: s3.client
        """
        s3_client = boto3.client("s3",
                                 aws_access_key_id=self.__aws_access_key,
                                 aws_secret_access_key=self.__aws_secret_key
                                 )
        logger.info("Created S3 client")
        return s3_client


def parser_arguments(args):
    """
    Parses give command line arguments and creates namespace with key-value.

    :parameter list args: List of valid arguments.
    :return: argparse.ArgumentParser namespace with key-value
    """
    # Creating parser object
    parser = ArgumentParser(description="Parser object to parser input arguments")
    parser.add_argument('search', type=str, help="Keyword/hashtag to search for")
    parser.add_argument('--count', type=int, default=200, help="Number for tweets to be pulled. Default is 200.")
    parser.add_argument('--env', choices=['local', 'remote'], default='local', help="Mode of running")
    return parser.parse_args(args)


if __name__ == '__main__':
    logger.info("Arguments passed to command line: {}".format([str(arg) for arg in sys.argv[1:]]))
    parsed_args = parser_arguments(sys.argv[1:])
    logger.info("Parsed arguments:")
    for arg in vars(parsed_args):
        logger.info("\t{}: {}".format(arg, getattr(parsed_args, arg)))
    logger.info("Creating twitter session")
    session = TwitterSession(parsed_args)
    logger.info("Calling ingestion function")
    twitter_etl.ingest(session)
