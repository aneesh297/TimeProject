import os
import time
import traceback
from typing import List

from TwitterAPI import TwitterAPI, TwitterOAuth, TwitterRequestError, TwitterConnectionError, HydrateType

from TwitterAPIWrapper import TwitterJSONWrapper, Tweet, TwitterUser
from TweetDBHandler import TweetDBHandler
from DBHandler import DBHandler
import stream_utils
import pandas as pd


class TwitterStream:
    EXPANSIONS = 'author_id,referenced_tweets.id,referenced_tweets.id.author_id,geo.place_id'
    TWEET_FIELDS = 'created_at,author_id'
    USER_FIELDS = 'location,profile_image_url,verified,public_metrics,description'
    PLACE_FIELDS = 'contained_within,country,country_code,full_name,geo,id,name,place_type'
    users: List[TwitterUser]
    tweets: List[Tweet]

    def __init__(self, db: DBHandler = None):
        self.db_handler = db
        # Twitter Credentials are stored in creds.txt
        o = TwitterOAuth.read_file("creds.txt")
        self.api = TwitterAPI(o.consumer_key, o.consumer_secret,
                              auth_type='oAuth2', api_version='2')
        self.metadata_fields = {
            'expansions': TwitterStream.EXPANSIONS,
            'tweet.fields': TwitterStream.TWEET_FIELDS,
            'user.fields': TwitterStream.USER_FIELDS,
            'place.fields': TwitterStream.PLACE_FIELDS
        }
        self.users = []
        self.tweets = []

    @staticmethod
    def gen_rules(rules):
        processed_rules = {"add": []}
        for rule in rules:
            processed_rules["add"].append({"value": f'\"{rule}\"'})
        return processed_rules

    # Update streaming rules on Twitter.
    def update_rules(self, rules):
        try:
            r = self.api.request('tweets/search/stream/rules', self.gen_rules(rules))
            # print(f'[{r.status_code}] RULES: {r.text}')
            print(f'[{r.status_code}]')

        except Exception as e:
            print(e)

    # Fetch streaming rules (keywords) from Twitter. Use for debugging.
    def get_rules(self):
        r = self.api.request('tweets/search/stream/rules', method_override='GET')
        print(f'[{r.status_code}]')
        return r.json()

    def delete_rules(self):
        rules = self.get_rules()
        if 'data' in rules:
            rules = rules['data']
        else:
            return
        ids = []
        for rule in rules:
            ids.append(rule['id'])
        delete_phrase = {"delete": {"ids": ids}}
        try:
            r = self.api.request('tweets/search/stream/rules', delete_phrase)
            #             print(f'[{r.status_code}] RULES: {r.text}')
            print(f'[{r.status_code}]')

        except Exception as e:
            print(e)

    def stream(self):
        try:
            r = self.api.request('tweets/search/stream', self.metadata_fields,
                                 hydrate_type=HydrateType.NONE)
            print(f'[{r.status_code}] START...')
            count = 0
            for item in r:
                data = TwitterJSONWrapper(item)
                self.store_tweet_to_db(data)
                if count % 10 == 0:
                    print(f"Count: {count}")
                count += 1

        except TwitterRequestError as e:
            print(e.status_code)
            for msg in iter(e):
                print(msg)
        except TwitterConnectionError as e:
            print(e)
        except KeyboardInterrupt:
            print("Keyboard interrupt. Stopping now")
            return KeyboardInterrupt
        except Exception as e:
            print(e)
            traceback.print_exc()

    def search_tweet(self, tweet_id):
        try:
            r = self.api.request(f'tweets/:{tweet_id}', self.metadata_fields,
                                 hydrate_type=HydrateType.NONE)
            print(r.json())
            print(r.get_quota())
        #             return r.json()
        except TwitterRequestError as e:
            print(e.status_code)
            for msg in iter(e):
                print(msg)

        except TwitterConnectionError as e:
            print(e)

        except Exception as e:
            print(e)

    def store_tweet_to_db(self, data):
        # self.users.extend(data.users)
        # self.tweets.extend(data.tweets)
        for tweet in data.tweets:
            TweetDBHandler.insert_tweet(tweet, self.db_handler)
        for user in data.users:
            TweetDBHandler.insert_user(user, self.db_handler)
        #     Dump to CSV files for debugging
        # if len(self.tweets) > 50:
        #     self.dump_to_file()
        #     self.users = []
        #     self.tweets = []

    # Stores tweets and users in csv files.
    def dump_to_file(self):
        print("Dumping to CSV files")
        user_df = pd.DataFrame([user.user_dict() for user in self.users])
        tweet_df = pd.DataFrame([tweet.tweet_dict() for tweet in self.tweets])
        user_file_count = tweet_file_count = 0

        while os.path.isfile(f"users_{user_file_count}.csv"):
            user_file_count += 1
        while os.path.isfile(f"tweets_{tweet_file_count}.csv"):
            tweet_file_count += 1
        print(f"tweets_{tweet_file_count}.csv", f"users_{user_file_count}.csv")
        user_df.to_csv(f"users_{user_file_count}.csv", index=False)
        tweet_df.to_csv(f"tweets_{tweet_file_count}.csv", index=False)


def main():
    db_credentials = stream_utils.read_database_credentials('db_creds.json')

    db = DBHandler()
    # Update credentials here
    db.create_db_connection(db_credentials['local_host'], db_credentials['local_user'],
                            db_credentials['local_password'], db_credentials['db_name'])
    streamer = TwitterStream(db)
    streamer.delete_rules()
    keywords = read_keywords(db)
    streamer.update_rules(keywords)
    while streamer.stream() != KeyboardInterrupt:
        print("Sleeping")
        time.sleep(5)


def read_keywords(db_handler: DBHandler):
    read_keyword_query = f"SELECT keyword from {TweetDBHandler.DATABASE_NAME}.twitter_keywords"
    keyword_tuples = db_handler.execute_read_query(read_keyword_query)
    keywords = [key[0] for key in keyword_tuples]
    return keywords


if __name__ == "__main__":
    main()
