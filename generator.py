import random
import datetime
import numpy


#  Import list of special and random words + random usernames
with open('special_words.csv') as f:
    special_words = f.readlines()
special_words = [x.strip() for x in special_words]
with open('standard_words.csv') as f:
    standard_words = f.readlines()
standard_words = [x.strip() for x in standard_words]
with open('usernames.csv') as f:
    usernames = f.readlines()
usernames = [x.strip() for x in usernames]


def generate_tweet(min_words=10, max_words=16):
    special_prob = [0.22, 0.17, 0.16, 0.11, 0.09, 0.07, 0.05, 0.05, 0.04, 0.04]
    num_words = random.randint(min_words, max_words)
    tweet = []
    for i in range(0, num_words):
        if random.randint(0, 100) < 7:
            # generate special word
            tweet.append(numpy.random.choice(special_words, p=special_prob))
        else:
            # generate standard word
            tweet.append(random.choice(standard_words))
    return ' '.join(tweet)


def generate_user():
    user_prob = [0.22, 0.17, 0.16, 0.11, 0.09, 0.07, 0.05, 0.05, 0.04, 0.04]
    user = numpy.random.choice(usernames, p=user_prob)
    return user


def generate_timestamp():
    return datetime.datetime.now().replace(microsecond=0).isoformat(' ')


def generate_post():
    return ','.join([generate_user(), generate_timestamp(), generate_tweet()])
