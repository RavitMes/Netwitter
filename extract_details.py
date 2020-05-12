"""
This module includes functions to scrape information from twitter directly via tweepy package
"""
import tweepy
import re
import dateutil.parser
from tqdm import tqdm
from config import API_DICT, API_DICT2
import logging
import time
import multiprocessing
import time
TEMP = 1

def verify_authentication(API_DICT):
    """
    :param API_DICT: dict including relevant keys in order to be able scrapping data using tweepy
    :return: api instant
    """
    global TEMP
    logger = logging.getLogger(__name__)
    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(API_DICT['consumer_key'], API_DICT['consumer_secret'])
    auth.set_access_token(API_DICT['access_token'],API_DICT['access_token_secret'])
    api = tweepy.API(auth, wait_on_rate_limit=True)
    TEMP = 2
    # test authentication
    try:
        api.verify_credentials()
        logger.info("Authentication OK")
    except:
        logger.warning(f"Error during authentication")
    return api

api = verify_authentication(API_DICT)

def change_api():
    global TEMP
    logger = logging.getLogger(__name__)
    logger.info(f"changing api due to scraping limitations")

    if TEMP == 1:
        api = verify_authentication(API_DICT)
        TEMP=2
    else:
        api = verify_authentication(API_DICT2)
        TEMP=1
    return api


def extract_personal_info(screen_name, api=api):
    """
    :param screen_name
    :return: tuple of personal information associated with the user including:
    user_id, name, screen_name, location, description, followers_num, following_num, profile_pic
    """
    logger = logging.getLogger(__name__)
    logger.info(f"start scraping personal details")
    user = api.get_user(screen_name)
    details =user._json
    print(details)
    user_id = details['id']
    name = details['name']
    screen_name = details['screen_name']
    location = details['location']
    description = details['description']
    followers_num = details['followers_count']
    following_num = details['friends_count']
    profile_pic= details['profile_image_url']
    is_protected=details['protected']
    return user_id, name, screen_name, location, description, followers_num, following_num, profile_pic,is_protected


def extract_followers(screen_name, api=api):
    """
    :param screen_name
    :return: list of users_id following after the given user
    """
    logger = logging.getLogger(__name__)
    logger.info(f"start scraping followers details")
    ids = []
    for page in tweepy.Cursor(api.followers_ids, screen_name=screen_name).pages(5000):
        ids.extend(page)
        time.sleep(60)

    return ids

def extract_following(screen_name, api=api):
    """
    :param screen_name and api
    :return: list of users_id the given user follows
    """
    logger = logging.getLogger(__name__)
    logger.info(f"start scraping following details")
    ids = []
    for page in tweepy.Cursor(api.friends_ids, screen_name=screen_name).pages():
        ids.extend(page)
        time.sleep(60)

    return ids

def extract_tweets_info(tweet, api=api) :
    """
    :param tweet and api
    :return: tuple including information associated with the given tweet:
    tweet_id, user_id, creation_date, content, tweet_lang, likes, retweeted_count,
    retweeted_from, kind, hashtags, mentioned_names, mentioned_urls
    """
    logger = logging.getLogger(__name__)
    logger.info(f"start scraping tweets")
    val=True
    while val:
        try:
            api=api
            tweet_info = tweet._json
            user_id =tweet_info['user']['id']
            date = tweet_info['created_at']
            creation_date= dateutil.parser.parse(date).strftime('%Y-%m-%d')
            tweet_id = tweet_info['id']
            tweet_lang = tweet_info['lang']
            status = api.get_status(tweet_id)._json
            likes = status['favorite_count']
            retweeted_count = status['retweet_count']
            text = tweet_info['text']
            # retweeted_from = 0
            # commented_users = []
            #
            # if text.startswith('RT @'):
            #     kind = "retweet"
            #     content=''
            #     try:
            #         retweeted_from = re.findall(f"(@.*): ",text)[0]
            #         # _, _, tweet_author, _, _, _, \
            #         # _, _, is_protected = extract_personal_info(retweeted_from)
            #         # print(is_protected)
            #         # if is_protected or retweeted_from=='@pyonghwabi':
            #         #     logger.info(f"user {tweet_author} protected, skip this tweet")
            #         #     return
            #         # else:
            #         content = re.findall("RT.*: (.*)",text)[0]
            #     except:
            #         pass
            #
            # elif text.startswith('@'):
            #     kind = "comment"
            #     commented_users = re.findall(f"(@[^\s]+)",text)
            #     splitted = text.split()
            #     content = ' '.join(splitted[len(commented_users):])
            #
            # else:
            #     kind = "tweet"
            #     content = text
            kind=""
            retweeted_from =""
            hashtags = tweet_info['entities']['hashtags']
            names = tweet_info['entities']['user_mentions']
            mentioned_names = []
            if len(names)>0:
                for name in names:
                    mentioned_names.append(name['screen_name'])
            # urls =[]
            # mentioned_urls = tweet_info['entities']['urls']
            # if len(mentioned_urls)>0:
            #     for url in mentioned_urls:
            #         urls.append(url['url'])

            mentioned_urls = []
            val = False
        except tweepy.TweepError:
            logger.warning("tweepy error")
            api = change_api()
            # wait 15 minutes
            # time.sleep(60 * 15)
        except:
            time.sleep(15*2)
    return tweet_id, user_id, creation_date, text, tweet_lang, likes, retweeted_count,\
          retweeted_from, kind, hashtags, mentioned_names, mentioned_urls
    # tweet_l.append((tweet_id, user_id, creation_date, content, tweet_lang, likes, retweeted_count,
    #         retweeted_from, kind, hashtags, mentioned_names, mentioned_urls))

def convert_screen_name_to_id(screen_name, api=api):
    """
    :param screen_name and api
    :return: user_id associated with the screen_name
    """
    logger = logging.getLogger(__name__)
    logger.info(f"converting screen_name to id")
    var=True
    while var:
        try:
            api=api
            user_id = api.get_user(screen_name).id
            var=False
        except tweepy.TweepError:
            api = change_api()
            # time.sleep(60 * 15)
        except:
            time.sleep(15*2)

    return user_id


def convert_id_to_screen_name(user_id, api=api):
    """
    :param user_id and api
    :return: screen name associated with the user name
    """
    logger = logging.getLogger(__name__)
    logger.info(f"converting id to screen_name")
    var = True
    while var:
        try:
            api = api
            screen_name = api.get_user(user_id).screen_name
            var=False
        except tweepy.TweepError:
            api=change_api()
            # time.sleep(60 * 15)
        except:
            time.sleep(15*2)

    return screen_name

def get_total_likes_stautuses(screen_name, api=api):
    """
    :param screen_name and api
    :return: tuple of user total number of likes and total number of statuses
    """
    logger = logging.getLogger(__name__)
    logger.info(f"get total_likes and total_statuses")
    user_id = api.get_user(screen_name).id
    timeline = api.user_timeline(user_id=user_id, count=200)
    user_total_likes=0
    user_total_statuses=0
    if len(timeline)>0:
        first_tweet = timeline[0]._json
        tweet_id = first_tweet['id']
        status = api.get_status(tweet_id)._json
        user_total_likes = status['user']['favourites_count']
        user_total_statuses = status['user']['statuses_count']
    return user_total_likes, user_total_statuses


def get_tweets(screen_name, api=api):
    """
    input: screen_name
    output: list of tuples with the following arguments:
    tweet_id, user_id, creation_date, content, tweet_lang, likes, retweeted_count, \
           retweeted_from, kind, hashtags, mentioned_names, mentioned_urls
    """
    logger = logging.getLogger(__name__)
    logger.info(f"scraping all tweets")
    if type(screen_name) is str and not screen_name.isdigit():
        user_id = api.get_user(screen_name).id
    else:
        user_id = screen_name
    # timeline = api.user_timeline(user_id=int(user_id), count=10)
    # print(timeline)

    # #
    # manager = multiprocessing.Manager()
    # return_list = manager.list()
    # jobs = []
    # for tweet in tweepy.Cursor(api.user_timeline, user_id=int(user_id)).items(30):
    #      p = multiprocessing.Process(target=extract_tweets_info, args=(tweet, return_list))
    #      jobs.append(p)
    #      p.start()
    #      logger.info("process....")
    #      p.join(60 * 5)
    #      #
    #      # If thread is still active
    #      if p.is_alive():
    #          logger.info(f'too much time to scrape tweets... continue to next user')
    #          # Terminate
    #          p.terminate()
    #          p.join()
    #
    # for proc in jobs:
    #     proc.join()
    #
    # return return_list

    tweets = []
    for tweet in tweepy.Cursor(api.user_timeline, user_id=int(user_id)).items(30):
        # if user_total_likes == '':
       # time.sleep(1)
       tweets.append(extract_tweets_info(tweet))
    return tweets


def find_tweets_by_keyword(keyword, api=api):
    """
    :param keyword:
    :return: list of tuples:
    tweet_id, user_id, creation_date, content, tweet_lang, likes, retweeted_count, \
           retweeted_from, kind, hashtags, mentioned_names, mentioned_urls
    """
    logger = logging.getLogger(__name__)
    logger.info(f"find tweets by keyword")
    #it is possible to limit the number of tweet by giving this variable inside the "items".
    num_of_tweets = 5000
    tweets = []
    for i, tweet in tqdm(enumerate(tweepy.Cursor(api.search, q=keyword, geocode="54.5260,105.2551,500km").items(num_of_tweets))):
        logging.info(f"extract tweet {i}")
        tweets.append(extract_tweets_info(tweet))
    return tweets
