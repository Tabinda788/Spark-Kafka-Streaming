	# from cgitb import text
# import tweepy
# import configparser

# # read configs
# config = configparser.ConfigParser()
# config.read('config.ini')

# api_key = config['twitter']['api_key']
# api_key_secret = config['twitter']['api_key_secret']

# access_token = config['twitter']['access_token']
# access_token_secret = config['twitter']['access_token']


# #  authentication
# auth = tweepy.OAuthHandler(api_key,api_key_secret)
# auth.set_access_token(access_token,access_token_secret)

# api = tweepy.API(auth,wait_on_rate_limit=True)

# # public_tweets = api.home_timeline()

# # print(public_tweets[0].text)

# # for tweet in public_tweets:
# #     print(tweet.text)
# search_query = "#covid19 -filter:retweets"

# # get tweets from the API
# tweets = tweepy.Cursor(api.search_tweets,
#               q=search_query,
#               lang="en",
#               since="2020-09-16").items(50)

# # store the API responses in a list
# tweets_copy = []
# for tweet in tweets:
#     tweets_copy.append(tweet)
    
# print("Total Tweets fetched:", len(tweets_copy))


# import tweepy

# auth = tweepy.OAuth1UserHandler(
#    "8Qzz4XIaCYnVPYzzaQ1IoiFmO", "W5EF5tI8yezceO3qNsyjjmJMUhaExjyRs1isELs5nkahvtuQUi", "1580054188477730818-Rv7I2WwGamgqecFJbEHIcTd5DPoYvT", "6CWaTmbBWB0JnPfb1w8h2njAzlFYSSQ238estGxz4Lbgv"
# )

# api = tweepy.API(auth)

# public_tweets = api.home_timeline()
# for tweet in public_tweets:
#     print(tweet.text)

# import requests

# url = "https://imdb8.p.rapidapi.com/auto-complete"

# querystring = {"q":"game of thr"}

# headers = {
# 	"X-RapidAPI-Key": "b280396dc2msh37ec4a647280689p12ee3ajsncc720ef4f947",
# 	"X-RapidAPI-Host": "imdb8.p.rapidapi.com"
# }

# response = requests.request("GET", url, headers=headers, params=querystring)

# # print(response.text)



# querystring = {"nconst":"nm0001667"}


# response = requests.request("GET", url, headers=headers, params=querystring)

# print("Get biography of actor or actress",response.text)


from youtube_statistics import YTstats
import configparser

# read configs
config = configparser.ConfigParser()
config.read('config.ini')

API_KEY = config['youtube']['api_key']
CHANNEL_ID = config['youtube']['channel_id']



yt = YTstats(API_KEY,CHANNEL_ID)
yt.get_channel_statistics()
yt.get_channel_video_data()
yt.dump()
# yt.get_channel_video_data()