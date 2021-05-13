from typing import List

from TwitterAPI import TwitterAPI, TwitterOAuth, TwitterRequestError, TwitterConnectionError, HydrateType
import stream_utils
from TwitterStream import TwitterStream
from math import ceil
import pandas as pd
import os.path
from TwitterAPIWrapper import TwitterJSONWrapper, TwitterUser, Tweet
import traceback
import time
import json


# Fields needed
# User data: userId, description, friendsCount, followersCount, screenName, statusesCount, location, name
# Tweet: id, createdAt, text, userId, isRetweet, latitude, longitude, place_country, place_name, place_type

class OldTweetGetter(TwitterStream):
    EXPANSIONS = 'author_id,referenced_tweets.id,referenced_tweets.id.author_id,geo.place_id'
    TWEET_FIELDS = 'created_at,author_id'
    USER_FIELDS = 'location,profile_image_url,verified,public_metrics,description'
    PLACE_FIELDS = 'contained_within,country,country_code,full_name,geo,id,name,place_type'
    tweet_data_file_name = "old_tweet_data.csv"
    users: List[TwitterUser]
    tweets: List[Tweet]

    def __init__(self, keywords):
        super().__init__()
        self.metadata_fields = {
            'expansions': OldTweetGetter.EXPANSIONS,
            'tweet.fields': OldTweetGetter.TWEET_FIELDS,
            'user.fields': OldTweetGetter.USER_FIELDS,
            'place.fields': OldTweetGetter.PLACE_FIELDS,
            'query': '',
            'start_time': '',
            'end_time': '',
            # 'next_token': '',
            'max_results': 500
        }
        self.users = []
        self.tweets = []
        self.queries = self.set_queries(keywords)
        self.last_query_checkpoint = ['' for i in range(len(self.queries))]
        print("Number of Query Strings: " + str(len(self.queries)))
        if os.path.isfile(self.tweet_data_file_name):
            self.df = pd.read_csv(self.tweet_data_file_name)
        else:
            self.df = pd.DataFrame()
            self.df.to_csv(index=False)

    def store_tweet(self, data):
        self.users.extend(data.users)
        self.tweets.extend(data.tweets)

        if len(self.tweets) > 100000:
            self.dump_to_file()
            self.users = []
            self.tweets = []

    def dump_to_file(self):
        print("Dumping to CSV files")
        user_df = pd.DataFrame([user.user_dict() for user in self.users])
        tweet_df = pd.DataFrame([tweet.tweet_dict() for tweet in self.tweets])
        user_file_count = tweet_file_count = 0

        while os.path.isfile(f"missing_users_{user_file_count}.csv"):
            user_file_count += 1
        while os.path.isfile(f"missing_tweets_{tweet_file_count}.csv"):
            tweet_file_count += 1
        print(f"missing_tweets_{tweet_file_count}.csv", f"missing_users_{user_file_count}.csv")
        user_df.to_csv(f"missing_users_{user_file_count}.csv", index=False)
        tweet_df.to_csv(f"missing_tweets_{tweet_file_count}.csv", index=False)

    def get_old_tweets(self, start_time, end_time, checkpoints_file_name=None):
        self.metadata_fields['start_time'] = start_time
        self.metadata_fields['end_time'] = end_time
        query_completed = [False for i in range(len(self.queries))]

        # To resume
        if checkpoints_file_name:
            print("Using old checkpoints")
            self.read_checkpoints_from_file(checkpoints_file_name)

        end_flag = False
        while True:
            for i in range(len(self.queries)):
                if not query_completed[i]:
                    try:
                        if len(self.last_query_checkpoint[i]) != 0:
                            self.metadata_fields['next_token'] = self.last_query_checkpoint[i]
                        elif 'next_token' in self.metadata_fields:
                            self.metadata_fields.pop('next_token')
                        self.metadata_fields['query'] = self.queries[i]

                        r = self.api.request('tweets/search/all', self.metadata_fields,
                                             hydrate_type=HydrateType.NONE)
                        response_json = r.json()
                        data = TwitterJSONWrapper(response_json)

                        if data.next_token:
                            self.last_query_checkpoint[i] = data.next_token
                        else:
                            query_completed[i] = True

                        if data.result_count == 0:
                            query_completed[i] = True
                        print(data.result_count)
                        self.store_tweet(data)

                        print(r.get_quota())
                        time.sleep(2.5)
                        # if r.get_quota()['remaining'] < 5:
                        #     # sleep
                        #     print("Rate limit reached. Sleeping for 15 min")

                    except TwitterRequestError as e:
                        print(e.status_code)
                        for msg in iter(e):
                            print(msg)

                    except TwitterConnectionError as e:
                        print(e)

                    except KeyboardInterrupt:
                        print("Keyboard interrupt. Stopping now")
                        end_flag = True
                        break

                    except Exception as e:
                        print(e)
                        traceback.print_exc()
            # Break if all queries have completed
            if sum(query_completed) == len(query_completed) or end_flag:
                break
        print("Number of Tweets collected: " + str(len(self.tweets)))
        print("Number of Users collected: " + str(len(self.users)))

    def store_checkpoints(self):
        with open('checkpoints.json', "w") as file:
            json.dump(self.last_query_checkpoint, file, indent=1)

    def read_checkpoints_from_file(self, file_name):
        with open(file_name, "r") as file:
            self.last_query_checkpoint = json.load(file)

    @staticmethod
    def set_queries(keywords):
        # query string can have only 1024 characters. The string has to be composed of the words joined by the OR
        # clause. Ex: "'nicotine' OR 'juul' OR 'vape' OR..."
        twitter_query_character_limit = 1024
        total_keyword_character_length = sum((len(keyword) + 4) for keyword in keywords)
        number_of_queries_required = ceil(total_keyword_character_length / twitter_query_character_limit)
        avg_query_length = ceil(total_keyword_character_length / number_of_queries_required)

        # TODO Sort Strings in alphabetical order for better performance?
        query = ""
        rule_strings = [query]
        for keyword in keywords:
            if len(query) > avg_query_length:
                query = f"\"{keyword}\" OR "
                rule_strings.append(query)
            else:
                query += f"\"{keyword}\" OR "
                rule_strings[-1] = query
        # Remove last OR
        for i in range(len(rule_strings)):
            query = rule_strings[i]
            rule_strings[i] = query[:-4]
            print(rule_strings[i])

        return rule_strings


def main():
    missing_keywords = stream_utils.read_keywords('missing_keywords.txt')
    streamer = OldTweetGetter(missing_keywords)
    streamer.get_old_tweets('2020-03-05T06:43:25Z', '2020-03-19T08:14:22Z')
    streamer.dump_to_file()


if __name__ == "__main__":
    main()
