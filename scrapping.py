"""
Scrapping module gets the scraped data from extract details
 according to the specified task and save it in global lists
these lists are then being saved gradually to the database by the functions in the db module.
"""
import tweepy
import logging
import extract_details as ex
import db
import time
from tqdm import trange
from tqdm import tqdm
import pandas as pd
import analyzewords_scraper as aw
import multiprocessing
USERS = []
FOLLOWERS = []
FOLLOWING = []
TWEET_LIST = []
RETWEET_LIST = []
HASHTAGS = []
MENTIONED_NAMES = []
MENTIONED_URLS = []
PERSONALITY=[]

def user_details(screen_name):
    """
    :param screen_name
    :return: dictionary "data" including the following information on the user:
    USER_ID, USER_NAME, SCREEN_NAME, LOCATION, DESCRIPTION, TOTAL_LIKES,
    TOTAL_STATUSES, FOLLOWERS (followers_num), FOLLOWING (following_num), FIG
    (profile picture url)
    """
    user_id, name, screen_name, location, description, followers_num, following_num, profile_pic, is_protected \
        = ex.extract_personal_info(screen_name=screen_name)
    user_total_likes, user_total_statuses = ex.get_total_likes_stautuses(screen_name=screen_name)
    data = {"USER_ID": user_id, "USER_NAME": name, "SCREEN_NAME": screen_name, "LOCATION": location,
            "DESCRIPTION": description, "TOTAL_LIKES": user_total_likes,
            "TOTAL_STATUSES": user_total_statuses, "FOLLOWERS": followers_num,
            "FOLLOWING": following_num, 'FIG': profile_pic}
    logger = logging.getLogger(__name__)
    logger.info("done extracting user details")
    return data


def update_users(data):
    """
    :param screen_name
    :return: updated USERS list (list of dicts, were each dict represents one user record)
    """
    global USERS
    USERS.append(data)


def update_following(screen_name):
    """
    Recieves twitter screen_name and update a list of followers by a new row (dict) representing
    new follower.
    :param screen_name
    :return: updated FOLLOWERS list (list of dicts, were each dict represents one FOLLOWER of the
    corresponding screen_name)
    """
    global FOLLOWING
    following_ids = ex.extract_following(screen_name=screen_name)
    user_id = ex.convert_screen_name_to_id(screen_name)
    for f in following_ids:
        data = {"USER_ID": user_id, "FOLLOWING_ID": f}
        FOLLOWING.append(data)


def update_followers(screen_name):
    """
    Recieves twitter screen_name and update a list of following by a new row (dict) representing
    new following.
    :param screen_name
    :return: updated FOLLOWING list (list of dicts, were each dict represents one FOLLOWING of the
    corresponding screen_name)
    """
    global FOLLOWERS
    followers_ids = ex.extract_followers(screen_name=screen_name)
    user_id = ex.convert_screen_name_to_id(screen_name)
    for f in followers_ids:
        data = {"USER_ID": user_id, "FOLLOWER_ID": f}
        FOLLOWERS.append(data)


def update_all_tweets(screen_name):
    """
    Recieves twitter screen_name and update a list of tweets by a new tweet (dict) representing
    new tweet corresponding to the screen_name (keys:user_id, tweet_id, creation_date,
    content, tweet_lang, likes, kind (retweet, comment or tweet))
    :param screen_name
    :return: updated TWEETS list (list of dicts, were each dict represents one tweet of the
    corresponding screen_name)
    """
    global TWEET_LIST
    global RETWEET_LIST
    global HASHTAGS
    global MENTIONED_NAMES
    global MENTIONED_URLS

    logger = logging.getLogger(__name__)

    tweets = ex.get_tweets(screen_name)
    logger.info(f"len of tweets for {screen_name} is: {len(tweets)}")
    # list of tuples with the following arguments: tweet_id, user_id, creation_date, content, tweet_lang,
    # likes, retweeted_count, retweeted_from, kind, hashtags, mentioned_names, mentioned_urls
    update_tweets_lists(tweets)


def update_personality(user_id, screen_name):
    """
    :param screen_name:
    :return: updateing PERSONALITY list with one dictionary representing one record, with the following
    keys: 'Upbeat','Worried','Angry' and 'Depressed' (representing the EMOTION aspect)
          'Plugged in','Personable', 'Arrogant/Distant' and 'Spacy' (representing the SOCIAL aspect)
          'Analytic', 'Sensory' and 'In-the-moment' (representing the THINKING aspect)
    """
    global PERSONALITY
    try:
        analyze_class = aw.Analyze_words_scraper(screen_name)
        emotional = analyze_class.get_emotional()
        social = analyze_class.get_social()
        thinking = analyze_class.get_thinking()
        personality = {**emotional, **social, **thinking}
        personality["user_id"]=user_id
        PERSONALITY.append(personality)
    except IndexError as err:
        logger = logging.getLogger(__name__)
        logger.error(f'{err}, personality scraping for {screen_name} failed')
        return


def update_tweets_lists(tweets):
    """
    :param tweets: list of tuples representing tweets corresponding to one user.
    each tuple represent one tweet of the user and contain the following information
    (according to the specified order):
    tweet_id, user_id, creation_date, content, tweet_lang,
    likes, retweeted_count, retweeted_from, kind, hashtags, mentioned_names, mentioned_urls
    :return: update global lists: TWEET_LIST, RETWEET_LIST, HASHTAGS, MENTIONED_NAMES, MENTIONED_URLS
    each element in these lists is a dict representing one tweet record
    """
# tweets
    print("enter")
    for tweet in tqdm(tweets):
        print(tweet)
        if tweet[7]!="":
            print("tweet: ", tweet[7])
            try:
                ex.convert_screen_name_to_id(tweet[7])
                data = {"user_id": tweet[1], "tweet_id": tweet[0], "creation_date": tweet[2],
                        "content": tweet[3],
                        "tweet_lang": tweet[4], "likes": tweet[5], "kind": tweet[8]}
            except:
                print(f'not scraping tweet info from {tweet[7]}')
                continue
        else:
            data = {"user_id": tweet[1], "tweet_id": tweet[0], "creation_date": tweet[2],
                    "content": tweet[3],
                    "tweet_lang": tweet[4], "likes": tweet[5], "kind": tweet[8]}
        TWEET_LIST.append(data)

# retweets
        if tweet[8] == 'retweet':
            try:
                author = ex.convert_screen_name_to_id(tweet[7].split()[0].strip(": "))
            except:
                author = 0
            data = {"tweet_id": tweet[0], "author_id": author, "retweet_count": tweet[6]}
            RETWEET_LIST.append(data)

#    hashtags
        if len(tweet[9]) != 0:
            for tag in tweet[9]:
                data = {"tweet_id": tweet[0], "hashtag": tag}
                HASHTAGS.append(data)

# mentioned names
        if len(tweet[10]) != 0:
            for tag in tweet[10]:
                data = {"tweet_id": tweet[0], "name": tag}
                MENTIONED_NAMES.append(data)

# mentioned urls
        if len(tweet[11]) != 0:
            for tag in tweet[11]:
                data = {"tweet_id": tweet[0], "url": tag['expanded_url']}
                MENTIONED_URLS.append(data)


def save_data(network, limit_followers, extract_tweets, extract_users):
    """
    Recieves network list and save to db personal details as well as tweets data
    relates to each user in that network in accordance to the specified task.
    if limit_followers is true - data regarding to the followers of each user will not be scraped.
    the personal details includes:
    user_id, name, screen_name, location, description, user_total_likes, user_total_statuses, fig.
    following and followers IDs list
    personality - including information about the following categories:
    Upbeat, Worried, Angry, and Depressed (representing the EMOTION aspect)
    Plugged in, Personable, Arrogant/Distant and Spacy (representing the SOCIAL aspect)
    Analytic, Sensory and In-the-moment (representing the THINKING aspect)
    Tweets details includes:
    tweet_id, user_id, creation_date, content, tweet_lang, likes,
    retweeted_count, retweeted_from, kind, hashtags, mentioned_names, mentioned_urls.
    :param network:
    :param limit_followers:
    :param task:
    :return: save details in the database).
    """
    global USERS
    global FOLLOWERS
    global FOLLOWING
    global TWEET_LIST
    global RETWEET_LIST
    global HASHTAGS
    global MENTIONED_NAMES
    global MENTIONED_URLS

    logger = logging.getLogger(__name__)
    logger.info(f"starting saving data")

    db_counter=1
    for i in trange(int(len(network)/2),len(network)):
        try:
            user = network[i]
            if extract_users:
                data = user_details(user)
                update_users(data)
                screen_name = data['SCREEN_NAME']
                update_personality(user,screen_name)
                update_following(screen_name)
                if not limit_followers:
                    update_followers(screen_name)
            if extract_tweets:
                screen_name=user
                update_all_tweets(screen_name)

            db_counter += 1
            db.write_data_to_db(USERS, FOLLOWERS, FOLLOWING, TWEET_LIST, RETWEET_LIST, HASHTAGS,
                                MENTIONED_NAMES, MENTIONED_URLS, PERSONALITY, db_counter)
            logger = logging.getLogger(__name__)
            logger.info(f"db counter {db_counter}. done saving the data for {screen_name}")
            USERS = []
            FOLLOWERS = []
            FOLLOWING = []
            TWEET_LIST = []
            RETWEET_LIST = []
            HASHTAGS = []
            MENTIONED_NAMES = []
            MENTIONED_URLS = []
        except tweepy.TweepError as err:
            # wait 15 minutes
            api = ex.change_api()
            logger = logging.getLogger(__name__)
            logger.error(f'{err}, changing api due to scraping limitations')
            #time.sleep(60 * 15)
            i -= 1
            continue
        except StopIteration:
            break


def extract_tweets_by_keyword(keyword, extract_users, limit_followers):
    """
    recieve keyword, extract all related tweets, and the network of users
    mentioned that key in their tweets and save it to database.
    executed in case the user chose for -get "requested_keyword".
    :param keyword
    :return: save tweets details and users used that key in the database
    """
    logger = logging.getLogger(__name__)
    logger.info(f"starting extract tweets by keyword")
    tweets = ex.find_tweets_by_keyword(keyword)
    # tweets return list of dict, where each dict is one row with the following values:
    # tweet_id, user_id, creation_date, content, tweet_lang, likes, retweeted_count, \
    # retweeted_from, kind, hashtags, mentioned_names, mentioned_urls
    update_tweets_lists(tweets)
    network = []
    for idx, record in enumerate(tweets):
        user_id = record[1]
        network.append(user_id)

    logger = logging.getLogger(__name__)
    logger.info(f"network of the people talking about {keyword} includes the following users:{network}")
    extract_tweets = True
    save_data(network, limit_followers, extract_tweets, extract_users)

def find_tweets_of_network(users_dataframe, extract_users, limit_followers):
    """
    gets a dataframe, extract from it the network (list of the column USER_ID),
    find all tweets associated with that network and save the data in the database
    """
    logger = logging.getLogger(__name__)
    logger.info(f"starting extract tweets of network")
    df = pd.read_csv(users_dataframe)
    df.dropna(subset=["USER_ID"],inplace=True)
    network = list(df["USER_ID"])
    # save_data(network,task="users",limit_followers=True)
    extract_tweets = True
    save_data(network, limit_followers, extract_tweets, extract_users)



def extract_network_of_user(screen_name, limit_followers, extract_tweets):
    """
    create network list of user(followers+following) and call save data
    function with that list as an input. if limit_followers is True,
    no data regarding the followers of each user in the network will be scraped
    (except for the total number of followers)
    :param screen_name, limit_followers(boolean)
    :return: create network (followers+following) list and call the
    function "save data" with that list as an input
    """

    followers_ids = ex.extract_followers(screen_name)
    following_ids = ex.extract_following(screen_name)
    network = [screen_name] + followers_ids + following_ids
    network = list(set(network))
    save_data(network, limit_followers, extract_tweets, extract_users=True)

