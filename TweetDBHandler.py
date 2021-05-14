from TwitterAPIWrapper import Tweet, TwitterUser
from DBHandler import DBHandler


# Utility methods that generates SQL queries for Twitter data and stores it in the DB using a DBHandler instance.
class TweetDBHandler:
    DATABASE_NAME: str = "tcorstwitter"
    @staticmethod
    def insert_tweet(tweet: Tweet, db_handler: DBHandler):
        query = (
            f"INSERT INTO {TweetDBHandler.DATABASE_NAME}.tweets (id, createdAt, text, userId, isRetweet, latitude, longitude, place_country, place_name, place_type) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        )
        # values = TweetDBHandler.gen_tweet_query_values(tweet)
        values = tweet.tweet_tuple()
        db_handler.execute_query_with_data(query, values)

    @staticmethod
    def insert_user(user: TwitterUser, db_handler: DBHandler):
        query = (
            f"REPLACE INTO {TweetDBHandler.DATABASE_NAME}.twitter_profiles(userId, description, friendsCount, followersCount, screenName, statusesCount, location, name) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        )
        values = user.user_tuple()
        db_handler.execute_query_with_data(query, values)
