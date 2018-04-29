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
        wordCount = 0
        charCount = 0
        post = line.split(",")
        words = post[2].split(" ")
        for posttext in post[2]:
            charCount = charCount+1
        for word in words:
            if(key_words.get(word, None) is not None):
                key_words[word] += 1
        wordCount = wordCount+len(words)
        if(data.get(post[0])):
            for word in key_words:
                data[post[0]][word] += key_words[word]
            data[post[0]]["wordCount"] += wordCount
            data[post[0]]["wordCount"] += charCount
            data[post[0]]["postCount"] += 1
        else:
            data[post[0]] = {}
            for word in key_words:
                data[post[0]][word] = key_words[word]
            data[post[0]]["wordCount"] = wordCount
            data[post[0]]["wordCount"] += charCount
            data[post[0]]["postCount"] = 1
