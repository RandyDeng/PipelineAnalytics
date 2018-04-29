from generator import special_words
from google.cloud import bigquery


def query_standard_sql(query):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    # Set use_legacy_sql to False to use standard SQL syntax.
    # Note that queries are treated as standard SQL by default.
    job_config.use_legacy_sql = False
    client.query(query, job_config=job_config)


def batch(messages):
    key_words = {}
    data = {}
    for line in messages:
        for sw in special_words:
            key_words[sw] = 0
        WordCount = 0
        CharCount = 0
        post = line.split(",")
        words = post[2].split(" ")
        for posttext in post[2]:
            CharCount = CharCount+1
        for word in words:
            if(key_words.get(word, None) is not None):
                key_words[word] += 1
        WordCount = WordCount+len(words)
        if(data.get(post[0])):
            for word in key_words:
                data[post[0]][word] += key_words[word]
            data[post[0]]["WordCount"] += WordCount
            data[post[0]]["CharCount"] += CharCount
            data[post[0]]["TweetCount"] += 1
        else:
            data[post[0]] = {}
            for word in key_words:
                data[post[0]][word] = key_words[word]
            data[post[0]]["WordCount"] = WordCount
            data[post[0]]["CharCount"] = CharCount
            data[post[0]]["TweetCount"] = 1
            data[post[0]]["StandardWords"] = 0
    for user in data:
#        print(user)
        query="INSERT INTO pipelineanalytics.word_count (Username,WordCount,CharCount, TweetCount, StandardWords, Randy_Deng, Saul_Crumpton, Eric_Daigrepont, Doug_Blough, Georgia_Tech, Google_Cloud, Baymax, Mario_Brothers, Marvel_vs_DC, Dungeons_and_Dragons) Values ('%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d);" %(user,data[user]["WordCount"],data[user]["CharCount"],data[user]["TweetCount"],data[user]["StandardWords"],data[user]["Randy_Deng"],data[user]["Saul_Crumpton"],data[user]["Eric_Daigrepont"],data[user]["Doug_Blough"],data[user]["Georgia_Tech"],data[user]["Google_Cloud"],data[user]["Baymax"],data[user]["Mario_Brothers"],data[user]["Marvel_vs_DC"],data[user]["Dungeons_and_Dragons"])
        print(query)
        query_standard_sql(query)        
