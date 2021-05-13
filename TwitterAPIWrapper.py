# Fields needed
# User data: userId, description, friendsCount, followersCount, screenName, statusesCount, location, name
# Tweet: id, createdAt, text, userId, isRetweet, latitude, longitude, place_country, place_name, place_type
from typing import List, Optional
import datetime


# Class definition of a Twitter Profile/User
class TwitterUser:
    def __init__(self):
        self.description: Optional[str] = ""
        self.user_id: Optional[str] = None
        self.friends_count: Optional[int] = None  # number of people the user is following.
        self.followers_count: Optional[int] = None
        self.screen_name: Optional[str] = None
        self.status_count: Optional[int] = None
        self.location: Optional[str] = None
        self.name: Optional[str] = None

    # TODO add quotes to strings
    def user_string(self):
        return f"{self.user_id}, {self.description}, {self.friends_count}, {self.followers_count}," \
               f" {self.screen_name}, {self.status_count}, {self.location}, {self.name}"

    def user_dict(self):
        return {
            "userId": self.user_id,
            "description": self.description,
            "friendsCount": self.friends_count,
            "followersCount": self.followers_count,
            "screenName": self.screen_name,
            "statusesCount": self.status_count,
            "location": self.location,
            "name": self.name
        }

    def user_tuple(self):
        return tuple([self.user_id, self.description, self.friends_count, self.followers_count,
                      self.screen_name, self.status_count, self.location, self.name])


# Class definition of a Tweet
class Tweet:
    def __init__(self):
        self.tweet_id: Optional[str] = ""
        self.created_at: Optional[str] = None
        self.text: Optional[str] = None
        self.user_id: Optional[str] = None
        self.is_retweet: Optional[int] = None
        self.latitude: Optional[float] = None
        self.longitude: Optional[float] = None
        self.place_country: Optional[str] = None
        self.place_name: Optional[str] = None
        self.place_type: Optional[str] = None

    def tweet_dict(self):
        return {
            "id": self.tweet_id,
            "createdAt": self.created_at,
            "text": self.text,
            "userId": self.user_id,
            "isRetweet": self.is_retweet,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "place_country": self.place_country,
            "place_name": self.place_name,
            "place_type": self.place_type
        }

    def tweet_string(self):
        value_string = f"'{self.tweet_id}','{self.created_at}','{self.text}'," \
                       f"'{self.user_id}',{self.is_retweet},{self.latitude},{self.longitude}," \
                       f"'{self.place_country}','{self.place_name}','{self.place_type}'"
        return value_string

    def tweet_tuple(self):
        return tuple([self.tweet_id, self.created_at, self.text, self.user_id, self.is_retweet, self.latitude,
                      self.longitude, self.place_country, self.place_name, self.place_type])


# Class for processing response json objects and converting them into Tweet and TwitterUser objects
class TwitterJSONWrapper:
    # variable type annotations
    response: dict
    includes: dict
    meta: dict
    result_count: int
    users: List[TwitterUser]
    tweets: List[Tweet]

    def __init__(self, response_json):
        self.response = response_json
        self.users = []
        self.tweets = []
        self.__process_response()

    def __process_response(self):
        self.meta = self.response.get('meta', {})
        self.data = self.response.get('data', [])
        self.includes = self.response.get('includes', {})

        self.result_count = self.meta.get('result_count', 1)
        self.next_token = self.meta.get('next_token', None)

        if type(self.data) == dict:
            self.data = [self.data]

        # Process Tweet data
        tweet_data: dict
        for tweet_data in self.data:
            tweet = Tweet()
            tweet.user_id = tweet_data.get('author_id', '')
            tweet.tweet_id = tweet_data.get('id', '')
            tweet.created_at = TwitterJSONWrapper.process_date(tweet_data.get('created_at', ''))
            tweet.text = tweet_data.get('text', '')
            tweet.is_retweet = 0
            if 'referenced_tweets' in tweet_data:
                for referenced_tweet in tweet_data['referenced_tweets']:
                    if referenced_tweet.get('type', '') == 'retweeted':
                        tweet.is_retweet = 1
                        break
            # TODO LAT LONG
            if 'geo' in tweet_data:
                place_id = tweet_data['geo'].get('place_id', '')
                place: dict
                for place in self.includes.get('places', []):
                    if place.get('id', '') == place_id:
                        tweet.place_name = place.get('full_name', '')
                        tweet.place_country = place.get('country', '')
                        tweet.place_type = place.get('place_type', '')

            self.tweets.append(tweet)

        # Process User data
        user_ids = set(tweet.user_id for tweet in self.tweets)
        user_data: dict
        for user_data in self.includes.get('users', []):
            if user_data.get('id', '') in user_ids:
                user = TwitterUser()
                user.user_id = user_data.get('id', '')
                user.description = user_data.get('description', '')
                # TODO def value to use for numeric data
                user.friends_count = user_data.get('public_metrics', {}).get('following_count', 0)
                user.followers_count = user_data.get('public_metrics', {}).get('followers_count', 0)
                user.screen_name = user_data.get('username', '')
                user.status_count = user_data.get('public_metrics', {}).get('tweet_count', 0)
                user.location = user_data.get('location', '')
                user.name = user_data.get('name', '')
                self.users.append(user)

    # Format ISO 8601 date to SQL Datetime
    @staticmethod
    def process_date(created_at):
        return datetime.datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
