import json
from generator import special_words

def batch(messages):
    key_words={}
    data={}
#    print(messages)
    for line in messages:
        for sw in special_words:
            key_words[sw]=0
        wordCount=0
        charCount=0
        #print(line)
        post=line.split(",")
        #print(post)
        words=post[2].split(" ")
        for posttext in post[2]:
            charCount=charCount+1
        for word in words:
            if(key_words.get(word,None)!= None):
                key_words[word]+=1
#        print(words)
        wordCount=wordCount+len(words)
        print(charCount)
        print(wordCount)
#        print(key_words)
        if(data.get(post[0])):
            for word in key_words:
                data[post[0]][word]+=key_words[word]
            data[post[0]]["wordCount"]+=wordCount
            data[post[0]]["wordCount"]+=charCount
            data[post[0]]["postCount"]+=1
        else:
            data[post[0]]={}
            for word in key_words:
                data[post[0]][word]=key_words[word]
            data[post[0]]["wordCount"]=wordCount
            data[post[0]]["wordCount"]+=charCount
            data[post[0]]["postCount"]=1
        print(data)
    return True
