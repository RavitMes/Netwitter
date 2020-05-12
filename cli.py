"""
This module contains the cli class, this class and parse_arguments_advanced.
It executes the program from the main function, according to the specified arguments given by the user.
the input arguments are parsed by the parse_arguments_advanced function
"""

import argparse
import logging
import scrapping as sc
from config import DF_NETWORK

class Cli:
    def __init__(self):
        self.args = None
        self.logger = logging.getLogger(__name__)

    def parse_arguments_advanced(self):
        """ Processing and storing the arguments of the program
            returns an argparse.Nampespace object, depicting and store the input arguments
            according to the defined flags
        """
        parser = argparse.ArgumentParser(
            description="Script Description"
        )
        parser.add_argument("-n", '--network')
        parser.add_argument("-k", "--keyword", help="""
                            scraping data of the requested keyword and save it in a 
                            csv and sql data base. 
                            Note: don't forget to add "" around keyword of >=2 words.

                            For example: -g "King Fisher" 
                            will output the tweets associated with the keyword "King Fisher"
                            """, nargs='+')

        self.args = parser.parse_args()

    def args_handel(self):
        """ The function handles the arguments """
        self.logger.info(f'Starting to  handel arguments')
        # task1("network"): extract network of user
        if self.args.network and self.args.keyword is None:
            sc.extract_network_of_user(screen_name=self.args.network, limit_followers=True, extract_tweets = True)
        # task2("keyword"): extract tweets by keyword
        elif self.args.keyword and self.args.network is None:
            logging.info(f"starting to scrape keyword {self.args.keyword[0]}")
            sc.extract_tweets_by_keyword(keyword=self.args.keyword[0], extract_users=True, limit_followers=True)
        # task 3("users"): extract tweets of pre-defined list of users
        else:
            sc.find_tweets_of_network(users_dataframe=DF_NETWORK,  extract_users=True, limit_followers=True)
