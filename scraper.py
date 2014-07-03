#!/usr/bin/env python

import scraperwiki
import json
import sys
import dumptruck
from datetime import datetime, timedelta
from collections import Counter
from functools import partial 
from sys import maxint
from urllib2 import URLError 
from httplib import BadStatusLine

def oauth_login():
# XXX: Go to http://twitter.com/apps/new to create an app and get values # for these credentials that you'll need to provide in place of these
# empty string values that are defined as placeholders.
# See https://dev.twitter.com/docs/auth/oauth for more information
# on Twitter's OAuth implementation.
    CONSUMER_KEY = 'gThRRbRVc1sPp2nYohqFuCqtk'
    CONSUMER_SECRET = '8ycYDDf0E4MpdplgsMqGyo8zJQqDeXO9OBKyLfPa9kcHyptunP'
    OAUTH_TOKEN = '2489249527-GoO6xgo3KGNTBDIdSMVeHoOBY134nL7GIRMxX7H'
    OAUTH_TOKEN_SECRET = 'SxuJKNQOWtNECMLIdEHlRn1KQCPNlZVi7UVbdNiOSWbTb'
    auth = scraperwiki.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET,CONSUMER_KEY, CONSUMER_SECRET)

    twitter_api = scraperwiki.Twitter(auth=auth) 
    return twitter_api
    # Sample usage
    twitter_api = oauth_login()
    # Nothing to see by displaying twitter_api except that it's now a
    # defined variable
    print twitter_api
    
def make_twitter_request(twitter_api_func, max_errors=10, *args, **kw):
    # A nested helper function that handles common HTTPErrors. Return an updated
    # value for wait_period if the problem is a 500 level error. Block until the
    # rate limit is reset if it's a rate limiting issue (429 error). Returns None
    # for 401 and 404 errors, which requires special handling by the caller.
    def handle_twitter_http_error(e, wait_period=2, sleep_when_rate_limited=True):
        if wait_period > 3600: # Seconds
            print >> sys.stderr, 'Too many retries. Quitting.' 
            raise e
        # See https://dev.twitter.com/docs/error-codes-responses for common codes
        if e.e.code == 401:
            print >> sys.stderr, 'Encountered 401 Error (Not Authorized)'
            return None
        elif e.e.code == 404:
            print >> sys.stderr, 'Encountered 404 Error (Not Found)'
            return None
        elif e.e.code == 429:
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)' 
            if sleep_when_rate_limited:
                print >> sys.stderr, "Retrying in 15 minutes...ZzZ..." 
                sys.stderr.flush()
                time.sleep(60*15 + 5)
                print >> sys.stderr, '...ZzZ...Awake now and trying again.' 
                return 2
            else:
                raise e # Caller must handle the rate limiting issue
        elif e.e.code in (500, 502, 503, 504):
            print >> sys.stderr, 'Encountered %i Error. Retrying in %i seconds' % \
(e.e.code, wait_period) 
            time.sleep(wait_period) 
            wait_period *= 1.5
            return wait_period
        else: 
            raise e
    wait_period = 2
    error_count = 0
    while True: 
        try:
            return twitter_api_func(*args, **kw) 
        except twitter.api.TwitterHTTPError, e:
            error_count = 0
            wait_period = handle_twitter_http_error(e, wait_period) 
            if wait_period is None:
                return
        except URLError, e:
            error_count += 1
            print >> sys.stderr, "URLError encountered. Continuing." 
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise
        except BadStatusLine, e:
            error_count += 1
            print >> sys.stderr, "BadStatusLine encountered. Continuing." 
            if error_count > max_errors:
# Sample usage
                print >> sys.stderr, "Too many consecutive errors...bailing out." 
                raise
    

def harvest_user_timeline(twitter_api, screen_name=None, user_id=None, max_results=1000): 
    assert (screen_name != None) != (user_id != None), \
    "Must have screen_name or user_id, but not both"
    kw = { # Keyword args for the Twitter API call 
        'count': 100,
        'trim_user': 'true',
        'include_rts' : 'true',
        'since_id' : 1
        }
        
    if screen_name:
        kw['screen_name'] = screen_name
    else:
        kw['user_id'] = user_id
    max_pages = 16
    results = []
    tweets = make_twitter_request(twitter_api.statuses.user_timeline, **kw) 
    if tweets is None: 
        tweets = []
    results += tweets
    print >> sys.stderr, 'Fetched %i tweets' % len(tweets)
    page_num = 1
    
    if max_results == kw['count']:
        page_num = max_pages # Prevent loop entry
    while page_num < max_pages and len(tweets) > 0 and len(results) < max_results:
    # Necessary for traversing the timeline in Twitter's v1.1 API: # get the next query's max-id parameter to pass in.
    # See https://dev.twitter.com/docs/working-with-timelines. 
        kw['max_id'] = min([ tweet['id'] for tweet in tweets]) - 1
        tweets = make_twitter_request(twitter_api.statuses.user_timeline, **kw)
        results += tweets
        print >> sys.stderr, 'Fetched %i tweets' % (len(tweets),)
        page_num += 1
    print >> sys.stderr, 'Done fetching tweets' 
    return results[:max_results]
    
def find_popular_tweets(twitter_api, statuses, retweet_threshold=3):
    # You could also consider using the favorite_count parameter as part of
    # this  heuristic, possibly using it to provide an additional boost to
    # popular tweets in a ranked formulation
    return [ status
                for status in statuses
                    if status['retweet_count'] > retweet_threshold ]

def extract_tweet_entities(statuses):
# See https://dev.twitter.com/docs/tweet-entities for more details on tweet
# entities
    if len(statuses) == 0: 
        return [], [], [], [], []
    screen_names = [ user_mention['screen_name'] 
                        for status in statuses
                            for user_mention in status['entities']['user_mentions'] ]
    hashtags = [ hashtag['text']
                    for status in statuses
                        for hashtag in status['entities']['hashtags'] ]
    urls = [ url['expanded_url']
                    for status in statuses
                        for url in status['entities']['urls'] ]
    symbols = [ symbol['text']
                    for status in statuses
                        for symbol in status['entities']['symbols'] ]
# In some circumstances (such as search results), the media entity # may not appear
    if status['entities'].has_key('media'):
        media = [ media['url']
                        for status in statuses
                            for media in status['entities']['media'] ]
    else:
        media = []
    return screen_names, hashtags, urls, media, symbols
    
    words=[w
            for t in status_texts
                for w in t.split().startswith('#')]
                    
def get_common_tweet_entities(statuses, entity_threshold=3):
    # Create a flat list of all tweet entities
    tweet_entities = [ e
                    for status in statuses
                        for entity_type in extract_tweet_entities([status]) 
                            for e in entity_type
                     ]
    c = Counter(tweet_entities).most_common()
    # Compute frequencies
    return [ (k,v)
            for (k,v) in c
                if v >= entity_threshold ]

def dropper(table):   
    if table!='':     
        try: scraperwiki.sqlite.execute('drop table "'+table+'"')     
        except: pass
    
dropper('swdata')

scraperwiki.sqlite.execute("create table if not exists 'aaa'(created_at int)")

twitter_api = oauth_login()
tweets = harvest_user_timeline(twitter_api, screen_name='rede_globo', \
    max_results=100)
popular_tweets = find_popular_tweets(twitter_api, tweets)
common_entities = get_common_tweet_entities(tweets)
for tweet in popular_tweets:
    for hashtag_count in tweet['entities']['hashtags']:
        print hashtag_count['text']
        print tweet['text'], tweet['retweet_count'], tweet['created_at']
        for objeto in tweet['created_at']:
            clean_timestamp = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
            offset_hours = -3 #offset in hours for EST timezone
        #account for offset from UTC using timedelta                                
            local_timestamp = clean_timestamp + timedelta(hours=offset_hours)
        #convert to am/pm format for easy reading
            final_timestamp =  datetime.strftime(local_timestamp, '%Y-%m-%d %I:%M:%S %p')
            final_timestamp2 =  datetime.strftime(local_timestamp, '%Y-%m-%d')  

            print final_timestamp
            dados={}
            dados['hashtag_trend']= hashtag_count['text']
            dados['created_at']=final_timestamp
            dados['dia']=final_timestamp2
            dados['retweet_count']=tweet['retweet_count']
            dados['favorite_count']=tweet['favorite_count']
            scraperwiki.sqlite.save(['created_at'], data=dados)
            scraperwiki.sqlite.save(['created_at'], data=dados, table_name ='aaa')
