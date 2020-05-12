# USER CONFIGURATIONS

# Path to CSV file to save output data
FILENAME_USER = 'user_network.csv'
FILENAME_TWEETS = 'network_tweets.csv'
TWEET_KEYWORD_FILE ="keyword_tweets.csv"

# Path to logger configuration file
LOG_CONF = 'logging.conf'

# Database name
DB_NAME = "unep_mutual_network"

# Database connection params - fill here your details
DB_HOST = "localhost"
DB_USER = "user"
DB_PASSWD = "password"

# API_DICT
API_DICT = {
'consumer_key': 'add_key_given_by_twitter',
'consumer_secret': 'add_consumer_secret_given_by_twitter',
'access_token': 'add_access_token_given_by_twitter',
'access_token_secret': 'add_access_token_secret_given_by_twitter'
}

# name of pre-existing network dataframe(for "users" task, see elaboration in main)
# this dataframe should have a column named "USER_ID"
DF_NETWORK = "df.csv"
