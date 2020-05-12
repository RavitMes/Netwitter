"""
This module handles the creation and management of the database:
     - creates new database if not exits
     - inserts data to database
 """
import logging
import mysql.connector as mysql
from config import DB_HOST, DB_USER, DB_PASSWD, TWEET_KEYWORD_FILE, DB_NAME


def connect_db():
    """ connects to db, returns connection and cursor
        Returns:
        db: connection to db
        cursor: database cursor
    """
    db = mysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD)
    cursor = db.cursor()
    return db, cursor


def delete_db():
    """ Deletes a database """
    db, cursor = connect_db()
    cursor.execute(f"DROP DATABASE {DB_NAME}")
    cursor.close()
    db.close()


def create_db(db_counter):
    """ Creates database and tables if not exists """
    global DB_NAME
    logger = logging.getLogger(__name__)
    logger.info("Create db and tables if not exists")
    try:
        db, cursor = connect_db()
        db_name = DB_NAME
        print(db_name)
    except:
        print('except on connect')
    try:
        if db_counter % 1000 == 0:
            i = int(db_counter / 1000)
            db_name = DB_NAME[:-1] + str(i)
            DB_NAME = db_name
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    except mysql.Error as err:
        cursor.close()
        db.close()
        logger = logging.getLogger(__name__)
        logger.error(f'Failed creating database: {err}"')
        raise Exception('DB error')

    try:
        # create tables
        cursor.execute(f"USE {DB_NAME}")
        cursor.execute("""CREATE TABLE IF NOT EXISTS TWEETS (\
                                            TWEET_ID BIGINT PRIMARY KEY AUTO_INCREMENT,\
                                            USER_ID BIGINT,\
                                            DATE TIMESTAMP,\
                                            CONTENT TEXT,\
                                            LANGUAGE VARCHAR(255),\
                                            LIKES INT,\
                                            KIND VARCHAR(255)\
                                            )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS RETWEETS (\
                                            TWEET_ID BIGINT,\
                                            AUTHOR_ID BIGINT,\
                                            RETWEET_COUNT INT\
                                            )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS HASHTAGS (\
                                            TWEET_ID BIGINT,\
                                            THEME TEXT\
                                            )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS MENTIONED_NAMES (\
                                            TWEET_ID BIGINT,\
                                            NAME VARCHAR(255)\
                                            )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS MENTIONED_URLS (\
                                             TWEET_ID BIGINT,\
                                             URL TEXT \
                                             )""")

        cursor.execute(f"""CREATE TABLE IF NOT EXISTS USERS ( \
                                             USER_ID BIGINT PRIMARY KEY AUTO_INCREMENT, \
                                             USER_NAME VARCHAR(255), \
                                             SCREEN_NAME VARCHAR(255), \
                                             LOCATION VARCHAR(255), \
                                             DESCRIPTION TEXT, \
                                             TOTAL_LIKES INT, \
                                             TOTAL_STATUSES INT, \
                                             FOLLOWERS INT,\
                                             FOLLOWING INT,\
                                             FIG TEXT \
                                             )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS FOLLOWERS ( \
                                             USER_ID BIGINT,\
                                             FOLLOWER_ID BIGINT \
                                             )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS FOLLOWING ( \
                                             USER_ID BIGINT, \
                                             FOLLOWING_ID BIGINT \
                                             )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS PERSONALITY ( \
                                             USER_ID BIGINT PRIMARY KEY AUTO_INCREMENT, \
                                             UPBEAT INT,\
                                             WORRIED INT,\
                                             ANGRY INT,\
                                             DEPRESSED INT, \
                                             PLUGGED_IN INT, \
                                             PERSONABLE INT,\
                                             DISTANT INT,\
                                             SPACY INT,\
                                             ANALYTIC INT,\
                                             SENSORY INT,\
                                             IN_THE_MOMENT INT\
                                             )""")

    except mysql.Error as err:
        logger = logging.getLogger(__name__)
        logger.error(f'Failed creating table: {err}"')
        raise Exception('DB error')
    finally:
        cursor.close()
        db.close()


def insert_personal_info_to_db(user_data, followers_data, following_data):
    """ Insert personal information of the user to the relevant tables in the database
        Parameters:
        user_data (list of dicts): each dict contain personal data regarding one user
        followers_data (list of dicts): each dict contain data regarding one user followers
        following_data (list of dicts): each dict contain data regarding the people one user follows
    """
    db, cursor = connect_db()
    logger = logging.getLogger(__name__)
    logger.info("Starting to insert data into db")
    try:
        cursor.execute(f"USE {DB_NAME}")
        for i, record in enumerate(user_data):
            insert_query = '''INSERT IGNORE INTO USERS (USER_ID, USER_NAME, SCREEN_NAME, 
            LOCATION, \
                            DESCRIPTION, TOTAL_LIKES, TOTAL_STATUSES, FOLLOWERS, FOLLOWING, FIG) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            print(record)
            record_values = (record['USER_ID'], record['USER_NAME'], record['SCREEN_NAME'],
                             record['LOCATION'], record['DESCRIPTION'], record['TOTAL_LIKES'],
                             record['TOTAL_STATUSES'], record['FOLLOWERS'], record['FOLLOWING'],
                             record['FIG'])
            cursor.execute(insert_query, record_values)
            if i % 100 == 0:
                db.commit()

        for i, record in enumerate(followers_data):
            insert_query = '''INSERT INTO FOLLOWERS (USER_ID, FOLLOWER_ID) \
                            VALUES (%s, %s)'''
            print(record)
            record_values = (record['USER_ID'], record['FOLLOWER_ID'])
            cursor.execute(insert_query, record_values)
            if i % 100 == 0:
                db.commit()

        for i, record in enumerate(following_data):
            insert_query = '''INSERT INTO FOLLOWING (USER_ID, FOLLOWING_ID) \
                            VALUES (%s, %s)'''
            print(record)
            record_values = (record['USER_ID'], record['FOLLOWING_ID'])
            cursor.execute(insert_query, record_values)
            if i % 100 == 0:
                db.commit()

        db.commit()
    except mysql.Error as err:
        logger = logging.getLogger(__name__)
        logger.error(f'Failed creating table: {err}"')
        raise Exception('DB error')
    finally:
        cursor.close()
        db.close()


def insert_tweets_to_db(tweets, retweets, hashtags,
                        mentioned_names, mentioned_urls):
    """ Insert tweets information of the user and it's network to the relevant tables in the
    database
         Parameters:
         tweets (list of dicts): each dict contain one tweet data
         retweets (list of dicts): each dict contain one tweet data relates to one tweet_id
         hashtags (list of dicts): each dict contain one hashtag data relates to one tweet_id
         mentioned_names (list of dicts): each dict contain one mentioned name data relates to
         one tweet_id
         mentioned_urls (list of dicts): each dict contain one mentioned url data relates to one
         tweet_id
     """

    db, cursor = connect_db()
    logger = logging.getLogger(__name__)
    logger.info("Starting to insert data into db")
    # try:
    cursor.execute(f"USE {DB_NAME}")
    for i, record in enumerate(tweets):
        try:
            insert_query = '''INSERT IGNORE INTO TWEETS (TWEET_ID, USER_ID, DATE,
            CONTENT,\
                            LANGUAGE,LIKES,KIND) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
            print(record)
            record_values = (record["tweet_id"], record["user_id"], record["creation_date"],
                           record["content"], record["tweet_lang"], record["likes"],
                           record["kind"])
            cursor.execute(insert_query, record_values)
            if i % 100 == 0:
                db.commit()
        except mysql.Error as err:
            logger = logging.getLogger(__name__)
            logger.error(f'Failed creating table: {err}"')
            pass

    for i, record in enumerate(retweets):
        try:
            insert_query = '''INSERT INTO RETWEETS (TWEET_ID, AUTHOR_ID,
            RETWEET_COUNT) \
                            VALUES (%s, %s, %s)'''
            record_values = (record["tweet_id"], record["author_id"], record["retweet_count"])
            cursor.execute(insert_query, record_values)
            if i % 100 == 0:
                db.commit()
        except mysql.Error as err:
            logger = logging.getLogger(__name__)
            logger.error(f'Failed creating table: {err}"')
            pass

    for i, record in enumerate(hashtags):
        try:
            print("h", record)
            insert_query = '''INSERT INTO HASHTAGS (TWEET_ID, THEME) \
                            VALUES (%s, %s)'''

            record_values = (record["tweet_id"], record['hashtag']['text'])

            cursor.execute(insert_query, record_values)
            if i % 100 == 0:
                db.commit()
        except mysql.Error as err:
            logger = logging.getLogger(__name__)
            logger.error(f'Failed creating table: {err}"')
            pass

    for i, record in enumerate(mentioned_names):
        try:
            insert_query = '''INSERT INTO MENTIONED_NAMES (TWEET_ID, NAME) VALUES (
            %s,
            %s)'''
            record_values = (record["tweet_id"], record["name"])
            cursor.execute(insert_query, record_values)

            if i % 100 == 0:
                db.commit()
        except mysql.Error as err:
            logger = logging.getLogger(__name__)
            logger.error(f'Failed creating table: {err}"')
            pass

    for i, record in enumerate(mentioned_urls):
        try:
            print("mu", record)
            insert_query = '''INSERT INTO MENTIONED_URLS (TWEET_ID, URL) VALUES (%s,
            %s)'''
            record_values = (record["tweet_id"], record['url'])
            print("row-urls", record_values)
            cursor.execute(insert_query, record_values)

            if i % 100 == 0:
                db.commit()
        except mysql.Error as err:
            logger = logging.getLogger(__name__)
            logger.error(f'Failed creating table: {err}"')
            pass

    db.commit()
    cursor.close()
    db.close()


def insert_emotional_info_to_db(personality):
    """ Insert personal information of the user to the relevant tables in the database
         Parameters:
         user_data (list of dicts): each dict contain personal data regarding one user
         followers_data (list of dicts): each dict contain data regarding one user followers
         following_data (list of dicts): each dict contain data regarding the people one user
         follows
     """
    db, cursor = connect_db()
    logger = logging.getLogger(__name__)
    logger.info("Starting to insert data into db")
    try:
        cursor.execute(f"USE {DB_NAME}")
        for i, record in enumerate(personality):
            insert_query = '''INSERT IGNORE INTO PERSONALITY (USER_ID, UPBEAT, WORRIED,\
                                    ANGRY, DEPRESSED, PLUGGED_IN, PERSONABLE, DISTANT, SPACY,\
                                    ANALYTIC, SENSORY, IN_THE_MOMENT) \
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            print(record)

            record_values = (record['user_id'], record['Upbeat'], record['Worried'],
                             record['Angry'], record['Depressed'], record['Plugged in'],
                             record['Personable'], record['Arrogant/Distant'], record['Spacy'],
                             record['Analytic'], record['Sensory'], record['In-the-moment'])
            cursor.execute(insert_query, record_values)
            if i % 100 == 0:
                db.commit()
        db.commit()
    except mysql.Error as err:
        logger = logging.getLogger(__name__)
        logger.error(f'Failed creating table: {err}"')
        raise Exception('DB error')
    finally:
        cursor.close()
        db.close()


def write_data_to_db(users, followers, following, tweets, retweets, hashtags,
                     mentioned_names, mentioned_urls, personality, db_counter):
    """ Creates db and tables if not exists, and inserts data into it using the above functions"""
    # delete_db()
    logger = logging.getLogger(__name__)
    logger.info('saving the data in the database')
    create_db(db_counter)
    insert_personal_info_to_db(users, followers, following)
    insert_emotional_info_to_db(personality)
    insert_tweets_to_db(tweets, retweets, hashtags, mentioned_names, mentioned_urls)

#
# def write_user_data_to_csv(users, followers, following, filename):
#     """ writes user data to csv """
#     data_total = []
#     for i, record in enumerate(users):
#         data = {"user_id": record["USER_ID"], "user_name": record["USER_NAME"],
#                 "screen_name": record["SCREEN_NAME"], \
#                 "location": record["LOCATION"], "description": record["DESCRIPTION"],
#                 "likes": record["TOTAL_LIKES"], \
#                 "statuses": record["TOTAL_STATUSES"], "following": list(), "followers": list(),
#                 'followers': record["FOLLOWERS"], 'following': record["FOLLOWING"]}
#         for row in following:
#             if row["USER_ID"] == record["USER_ID"]:
#                 data["following"].append(row["FOLLOWING_ID"])
#         for row in followers:
#             if row["USER_ID"] == record["USER_ID"]:
#                 data["followers"].append(row["FOLLOWER_ID"])
#         data = {k: v.encode(encoding='UTF-8') if type(v) == str else v for k, v in data.items()}
#         data_total.append(data)
#
#     print(data)
#     with open(filename, 'a', newline='') as csv_output:
#         csv_writer = csv.DictWriter(csv_output, data.keys())
#         # if not is_file_exists:
#         csv_writer.writeheader()
#         csv_writer.writerows(data_total)
#
#
# def write_tweets_data_to_csv(tweets, retweets, hashtags,
#                              mentioned_names, mentioned_urls, filename):
#     """ writes tweets data to csv """
#     data_total = []
#     for i, record in enumerate(tweets):
#         data = {"tweet_id": record['tweet_id'], "user_id": record["user_id"],
#                 "date": record['creation_date'], \
#                 "content": record['content'], "language": record['tweet_lang'],
#                 "likes": record['likes'], \
#                 "kind": record['kind'], "hashtags": [], "author": record["user_id"],
#                 "mentioned_names": [], "mentioned_urls": []}
#
#         if data['kind'] == "retweet":
#             for row in retweets:
#                 print(row)
#                 print("rt", row["tweet_id"])
#                 print("dt", data["tweet_id"])
#                 if row["tweet_id"] == data["tweet_id"]:
#                     data["retweet_count"] = row["retweet_count"]
#                     data["author"] = row["author_id"]
#         for row in hashtags:
#             if row["tweet_id"] == data["tweet_id"]:
#                 data["hashtags"].append(row["hashtag"]['text'])
#         for row in mentioned_names:
#             if row["tweet_id"] == data["tweet_id"]:
#                 data["mentioned_names"].append(row["name"])
#         for row in mentioned_urls:
#             if row["tweet_id"] == data["tweet_id"]:
#                 data["mentioned_urls"].append(row["url"]["expanded_url"])
#
#         data = {k: v.encode(encoding='UTF-8') if type(v) == str else v for k, v in data.items()}
#         data_total.append(data)
#
#     print(data)
#     with open(filename, 'a', newline='') as csv_output:
#         csv_writer = csv.DictWriter(csv_output, data.keys())
#         csv_writer.writeheader()
#         csv_writer.writerows(data_total)
#
#
# def write_tweets_to_csv(tweets, filename=TWEET_KEYWORD_FILE):
#     """
#     :param tweets: list of dicts, where each dict represent one record
#     :param filename: the file to save the data in
#     :return:
#     """
#     for i in range(len(tweets)):
#         tweets[i] = {k: v.encode(encoding='UTF-8') if type(v) == str else v for k, v in
#                      tweets[i].items()}
#
#     with open(filename, 'a', newline='') as csv_output:
#         csv_writer = csv.DictWriter(csv_output, tweets.keys())
#         csv_writer.writeheader()
#         csv_writer.writerows(tweets)
#
#
# def insert_keyword_tweets_to_db(tweets):
#     """
#     :param tweets:
#     :return:
#     """
#     create_db(db_counter=0)
#     db, cursor = connect_db()
#     logger = logging.getLogger(__name__)
#     logger.info("Starting to insert data into db")
#     try:
#         cursor.execute(f"USE {DB_NAME}")
#         for idx, record in enumerate(tweets):
#             insert_query_recipes = '''INSERT IGNORE INTO TWEETS (TWEET_ID, USER_ID, DATE,
#             CONTENT,\
#                             LANGUAGE,LIKES,KIND) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
#             print(record)
#             # tweet_id, user_id, creation_date, content, tweet_lang, likes, retweeted_count, \
#             # retweeted_from, kind, hashtags, mentioned_names, mentioned_urls
#             row_recipes = (record[0], record[1], record[2],
#                            record[3], record[4], record[5], record[8])
#             cursor.execute(insert_query_recipes, row_recipes)
#             if idx % 100 == 0:
#                 db.commit()
#
#             insert_query_recipes = '''INSERT INTO RETWEETS (TWEET_ID, AUTHOR_ID, RETWEET_COUNT) \
#                             VALUES (%s, %s, %s)'''
#             row_recipes = (record[0], ex.convert_screen_name_to_id(record[7]), record[6])
#             cursor.execute(insert_query_recipes, row_recipes)
#             if idx % 100 == 0:
#                 db.commit()
#
#             for i in record[9]:
#                 insert_query_recipes = '''INSERT INTO HASHTAGS (TWEET_ID, THEME) \
#                                 VALUES (%s, %s)'''
#
#                 row_recipes = (record[0], i)
#
#                 cursor.execute(insert_query_recipes, row_recipes)
#                 if idx % 100 == 0:
#                     db.commit()
#
#             for i in record[10]:
#                 insert_query_recipes = '''INSERT INTO MENTIONED_NAMES (TWEET_ID, NAME) VALUES (
#                 %s, %s)'''
#
#                 print("row-names", row_recipes)
#                 row_recipes = (record[0], i['expanded_url'])
#                 cursor.execute(insert_query_recipes, row_recipes)
#
#                 if idx % 100 == 0:
#                     db.commit()
#
#             for i in record[11]:
#                 insert_query_recipes = '''INSERT INTO MENTIONED_URLS (TWEET_ID, URL) VALUES (%s,
#                 %s)'''
#                 row_recipes = (record[0], i)
#                 print("row-urls", row_recipes)
#                 try:
#                     cursor.execute(insert_query_recipes, row_recipes)
#                 except:
#                     row_recipes = ("", i)
#                     cursor.execute(insert_query_recipes, row_recipes)
#                 if idx % 100 == 0:
#                     db.commit()
#
#         db.commit()
#     except mysql.Error as err:
#         logger = logging.getLogger(__name__)
#         logger.error(f'Failed creating table: {err}"')
#         raise Exception('DB error')
#     finally:
#         cursor.close()
#         db.close()
