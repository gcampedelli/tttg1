#!/usr/bin/env python

import scraperwiki
import json
from datetime import datetime, timedelta
from collections import Counter
from collections import OrderedDict
import smtplib

def oauth_login():
# XXX: Go to http://twitter.com/apps/new to create an app and get values # for these credentials that you'll need to provide in place of these
# empty string values that are defined as placeholders.
# See https://dev.twitter.com/docs/auth/oauth for more information
# on Twitter's OAuth implementation.
    CONSUMER_KEY = 'hwAUGkBG81qF7NL2Y4n5N8rJg'
    CONSUMER_SECRET = 'd42vsstMhfe6xqS1cwZIzK1ChEkMhPmnA3hXxaxlcHpgwQ1AnP'
    OAUTH_TOKEN = '25759477-9oTbL8u08E3ebg71rhGbl4NBo97fvdga2qlFxjRJk'
    OAUTH_TOKEN_SECRET = 'fvQhBFkdnn7yhdhZ18JKwbDto35sTaF4Z6SG8ilhrNTww'
    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET,CONSUMER_KEY, CONSUMER_SECRET)

    twitter_api = twitter.Twitter(auth=auth) 
    return twitter_api
    # Sample usage
    twitter_api = oauth_login()
    # Nothing to see by displaying twitter_api except that it's now a
    # defined variable
    print twitter_api

def twitter_search(twitter_api, q, lang, max_results=1000, **kw):
    # See https://dev.twitter.com/docs/api/1.1/get/search/tweets and
    # https://dev.twitter.com/docs/using-search for details on advanced
    # search criteria that may be useful for keyword arguments
    # See https://dev.twitter.com/docs/api/1.1/get/search/tweets
    search_results = twitter_api.search.tweets(q=q, lang=lang, count=1000, **kw)
    statuses = search_results['statuses']
    # Iterate through batches of results by following the cursor until we
    # reach the desired number of results, keeping in mind that OAuth users
    # can "only" make 180 search queries per 15-minute interval. See
    # https://dev.twitter.com/docs/rate-limiting/1.1/limits
    # for details. A reasonable number of results is ~1000, although
    # that number of results may not exist for all queries.
    # Enforce a reasonable limit
    max_results = min(1000, max_results)
    for _ in range(100): # 10*100 = 1000
        try:
            next_results = search_results['search_metadata']['next_results'] 
        except KeyError, e: # No more results when next_results doesn't exist
            break
# Create a dictionary from next_results, which has the following form: # ?max_id=313519052523986943&q=NCAA&include_entities=1
        kwargs = dict([ kv.split('=')
                        for kv in next_results[1:].split("&") ]) 
        search_results = twitter_api.search.tweets(**kwargs)
        statuses += search_results['statuses'] 
        if len(statuses) > max_results:
            break 
    return statuses

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
 
    return screen_names, hashtags, urls
    
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

def find_popular_tweets(twitter_api, statuses, retweet_threshold=3):
    # You could also consider using the favorite_count parameter as part of
    # this  heuristic, possibly using it to provide an additional boost to
    # popular tweets in a ranked formulation
    return [ status
                for status in statuses
                    if status['retweet_count'] > retweet_threshold ]

def dropper(table):   
    if table!='':     
        try: scraperwiki.sqlite.execute('drop table "'+table+'"')     
        except: pass
    
dropper('swdata')

t = oauth_login()
WORLD_WOE_ID = 23424768 # The Yahoo! Where On Earth ID for the entire world
my_trends = t.trends.place(_id=WORLD_WOE_ID) # get back a callable

# call the callable and iterate through the trends returned
print json.dumps(my_trends)
for item in my_trends:
    query = [trend['query'] for trend in item ['trends'] if trend['query']]
    name = [trend['name'] for trend in item ['trends'] if trend['name']]
    qname=[name[0], name[1], name[2], name[3], name[4], name[5], name[6], name[7], name[8], name[9]]
    myarray = [query[0], query[1], query[2], query[3], query[4], query[5], query[6], query[7], query[8], query[9]]
    mylist = dict(zip(qname,myarray))
    

for name, query in mylist.iteritems():
    print query
    twitter_api = oauth_login()
    results = twitter_search(twitter_api, q=query, lang="pt", \
    max_results=1000)
    screen_names, hashtags, urls = extract_tweet_entities(results)
    popular_tweets = find_popular_tweets(twitter_api, results)
    print query
    for tweet in popular_tweets:
        print tweet['text']
        for objeto in tweet['created_at']:
            clean_timestamp = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
            offset_hours = -3 #offset in hours for EST timezone
        #account for offset from UTC using timedelta                                
            local_timestamp = clean_timestamp + timedelta(hours=offset_hours)
        #convert to am/pm format for easy reading
            final_timestamp =  datetime.strftime(local_timestamp, '%Y-%m-%d %I:%M:%S %p')
            final_timestamp2 =  datetime.strftime(local_timestamp, '%Y-%m-%d')
            data={}
            data['TT']=name
            data['busca']=query
            data['text']=tweet['text']
            data['created_at']=final_timestamp
            scraperwiki.sqlite.save(["created_at"], data=data)

scraperwiki.sqlite.execute("DROP TABLE IF EXISTS 'aaa'")
scraperwiki.sqlite.execute("create table if not exists 'aaa'(TT int)")
scraperwiki.sqlite.execute("create table if not exists 'bbb'(TT int)")
results = scraperwiki.sql.select("* FROM swdata WHERE text in ( SELECT text FROM swdata WHERE text LIKE '%globo%' OR text LIKE '%GloboNaCopa' OR text LIKE '%Clarina%')")
print results
for key in results:
    dados={}
    dados['TT']=key['TT']
    dados['hora'] = key['created_at']
    print dados
    scraperwiki.sqlite.save(['TT'], data=dados, table_name ="aaa")
    scraperwiki.sqlite.save(['TT'], data=dados, table_name ="bbb")
if results == []:
    print 'sem resultados'
    exit()
else:
    print 'Existe TT para a Globo V.3 - Beta'


result2 = scraperwiki.sqlite.execute("select TT FROM aaa group by TT")

result2 = result2['data']
htmlcodes = ['&Aacute;', '&aacute;', '&Agrave;', '&Acirc;', '&agrave;', '&Acirc;', '&acirc;', '&Auml;', '&auml;', '&Atilde;', '&atilde;', '&Aring;', '&aring;', '&Aelig;', '&aelig;', '&Ccedil;', '&ccedil;', '&Eth;', '&eth;', '&Eacute;', '&eacute;', '&Egrave;', '&egrave;', '&Ecirc;', '&ecirc;', '&Euml;', '&euml;', '&Iacute;', '&iacute;', '&Igrave;', '&igrave;', '&Icirc;', '&icirc;', '&Iuml;', '&iuml;', '&Ntilde;', '&ntilde;', '&Oacute;', '&oacute;', '&Ograve;', '&ograve;', '&Ocirc;', '&ocirc;', '&Ouml;', '&ouml;', '&Otilde;', '&otilde;', '&Oslash;', '&oslash;', '&szlig;', '&Thorn;', '&thorn;', '&Uacute;', '&uacute;', '&Ugrave;', '&ugrave;', '&Ucirc;', '&ucirc;', '&Uuml;', '&uuml;', '&Yacute;', '&yacute;', '&yuml;', '&copy;', '&reg;', '&trade;', '&euro;', '&cent;', '&pound;', '&lsquo;', '&rsquo;', '&ldquo;', '&rdquo;', '&laquo;', '&raquo;', '&mdash;', '&ndash;', '&deg;', '&plusmn;', '&frac14;', '&frac12;', '&frac34;', '&times;', '&divide;', '&alpha;', '&beta;', '&infin']
funnychars = ['\xc1','\xe1','\xc0','\xc2','\xe0','\xc2','\xe2','\xc4','\xe4','\xc3','\xe3','\xc5','\xe5','\xc6','\xe6','\xc7','\xe7','\xd0','\xf0','\xc9','\xe9','\xc8','\xe8','\xca','\xea','\xcb','\xeb','\xcd','\xed','\xcc','\xec','\xce','\xee','\xcf','\xef','\xd1','\xf1','\xd3','\xf3','\xd2','\xf2','\xd4','\xf4','\xd6','\xf6','\xd5','\xf5','\xd8','\xf8','\xdf','\xde','\xfe','\xda','\xfa','\xd9','\xf9','\xdb','\xfb','\xdc','\xfc','\xdd','\xfd','\xff','\xa9','\xae','\u2122','\u20ac','\xa2','\xa3','\u2018','\u2019','\u201c','\u201d','\xab','\xbb','\u2014','\u2013','\xb0','\xb1','\xbc','\xbd','\xbe','\xd7','\xf7','\u03b1','\u03b2','\u221e']

newtext = ''

for textcontent in result2:
    for char in textcontent:
        if char not in funnychars:
            newtext = newtext + ", " + char
        else:
            newtext  = newtext +", "+ htmlcodes[funnychars.index(char)]
print newtext

to = ['gabriela@sagaz.in']
gmail_user = 'alertatttwitter@gmail.com'
gmail_pwd = 'alertaglobottt'
smtpserver = smtplib.SMTP("smtp.gmail.com",587)
smtpserver.ehlo()
smtpserver.starttls()
smtpserver.ehlo
smtpserver.login(gmail_user, gmail_pwd)
header = 'To:' + ", ".join(to) + '\n' + 'From: ' + gmail_user + '\n' + 'Subject:Alerta email TT Globo Twitter - BRASIL - V.5 Beta \n'
print header
msg = header + '\n Existe TT Globo no BRASIL \n\n' + newtext[1:].encode('ISO-8859-1')
smtpserver.sendmail(gmail_user, to, msg)
print 'done!'
to = ['alertatttwitter@gmail.com']
gmail_user = 'alertatttwitter@gmail.com'
gmail_pwd = 'alertaglobottt'
smtpserver = smtplib.SMTP("smtp.gmail.com",587)
smtpserver.ehlo()
smtpserver.starttls()
smtpserver.ehlo
smtpserver.login(gmail_user, gmail_pwd)
header = 'To:' + ", ".join(to) + '\n' + 'From: ' + gmail_user + '\n' + 'Subject:Alerta email TT Globo Twitter - BRASIL - V.5 Beta \n'
print header
msg = header + '\n Existe TT Globo no BRASIL \n\n' + newtext[1:].encode('ISO-8859-1')
smtpserver.sendmail(gmail_user, to, msg)
print 'done!'
smtpserver.close()
