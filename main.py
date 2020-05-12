"""
The main module for "netwitter scraper".
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

written by: Ravit Mesika
"""
import logging
import logging.config
from cli import Cli
from config import LOG_CONF


def main():
    """ The main function executes the program """
    logging.config.fileConfig(LOG_CONF)
    cli = Cli()
    cli.parse_arguments_advanced()
    cli.args_handel()


if __name__ == '__main__':
    main()





