"""
class Analyze_words_scraper gets a screen name and returns personality dictionaries
describing different aspects of the users personality: emotional, social and thinking.
"""

import requests
from bs4 import BeautifulSoup

from constants import URL, PARSER, EMOTION_LIST, SOCIAL_LIST, THINKING_LIST

class Analyze_words_scraper:
    def __init__(self, my_handle):
        handle_url = URL + my_handle
        result = requests.get(handle_url)
        self.my_soup = BeautifulSoup(result.content, PARSER)
        self.values_parse = self.my_soup.find_all('div', align="right")

    def fetch_values(self, value_index, key_list):
        value_dict = {}
        parsed_list = self.values_parse[value_index].find_all('td', align="right")

        for ind, item in enumerate(key_list):
            value_dict[item] = int(parsed_list[ind].text)
        return value_dict

    def get_emotional(self, emotion_list=EMOTION_LIST):
        emotional_dict = self.fetch_values(0, emotion_list)
        return emotional_dict

    def get_social(self, social_list=SOCIAL_LIST):
        social_dict=self.fetch_values(1, social_list)
        return social_dict
    
    def get_thinking(self, thinking_list=THINKING_LIST):
        thinking_dict=self.fetch_values(2, thinking_list)
        return thinking_dict
