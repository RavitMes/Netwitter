**Web scraping**

netwitter scraper is a scrapping program, enables executing the following tasks:
1. "network" - extracting information on the network of a given user
2. "keyword" - extracting all tweets associated with a desired keyword/keywords
3. "users" - extracting all tweets associated with a list of users

The information scraped including the following:

personal information:
user_id, name, screen_name, location, description, user_total_likes, user_total_statuses, fig.

network information:
following and followers IDs list

personality information (scraped from the analyzewords site)- including information
about the following three aspects of the human's personality: emotion, social and thinking
emotion : Upbeat, Worried, Angry, and Depressed
social: Plugged in, Personable, Arrogant/Distant and Spacy
thinking: Analytic, Sensory and In-the-moment.

Tweets information:
tweet_id, user_id, creation_date, content, tweet_lang, likes, retweeted_count, retweeted_from,
kind(tweet/retweet/comment), hashtags, mentioned_names, mentioned_urls.


**Requirements**

All requirements are specified in the file "requirements.txt"

**Usage**

In order to run the program, it is necessary to add the local mySQL password in the config.py file.
Changing the parameters in the config file according to the specified task

To Run the program

1."network": $python main.py -n "screen_name"
2. "keyword": $python main.py -k "keyword"
3. "users": $python main.py
for task "users" don't forget to update the DF_NETWORK variable in the config file and make sure to have a column names USER_ID containing the ids of the users of interest.


**Authors**

Ravit M, ravit2244@gmail.com
