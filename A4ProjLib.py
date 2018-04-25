import argparse
import apache_beam as beam
import os
import csv
import itertools
import random
import string
import time
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

#Generates a random Alphanumeric String up to the given size
def RandAlphaNumeric(size=6, chars=string.ascii_uppercase +string.ascii_lowercase +string.digits):
   return ''.join(random.choice(chars) for _ in range(size))

#Randomly select a Date/Time inside the given Date/Time Range
def RandSelectDateTime(start, end, format, prop):
   stime = time.mktime(time.strptime(start, format))
   etime = time.mktime(time.strptime(end, format))
   #       start time + (0to1)*(delta time)
   ptime = stime + prop*(etime - stime)
   return time.strftime(format, time.localtime(ptime))

#Generate a random Date/Time Stamp
def RandomDateTime(start, end, prop):
   return RandSelectDateTime(start, end, '%m/%d/%Y %I:%M %p', prop)

#Generate a random sentence up to a max number of word, truncating if needed
#   (e.g. In the case of simulating Twitter, Tweets max at 144 characters)
def RandomSentence(words, maxWords=15, truncate=0):
   NumWords = random.randint(1, maxWords)
   temp_str = ""
   for i in range(1, NumWords+1):
      temp_str = temp_str + words[random.randint(1,len(words)-1)]
      if (i<NumWords):
         temp_str = temp_str + " "
   #print (str(NumWords)+" word str: >>"+temp_str[0:truncate]+"<<"+str(len(temp_str)))
   if (truncate <= 0):   #return entire random (psuedo) sentence
      return (temp_str)
   else:                 #return truncated random (psuedo) sentence
      return (temp_str[0:truncate])

#Generate a random social media post
def RandomPost(sentence):
   randUser = RandAlphaNumeric(random.randint(5,20))
   randDT = RandomDateTime("1/1/2010 12:01 AM", "05/01/2018 11:59 PM", random.random())
   post = randUser +','+ randDT +','+ sentence
   return (post)
   
#Parse the csv from a single string to a 1-D array
def ParseFile(words):
   reader = csv.reader([words], delimiter=',')
   words = next(reader)
   return (words)

#Display the data line being handled (primarily for Troubleshooting)
def Disp(Sentence):
   print (Sentence)
   return (Sentence)

#Append the given new line to the requested file
def AppendOutputFile(newline, SamplePostFilename):
  writenewline = newline
  if os.path.exists(SamplePostFilename):
     writenewline = "\n"+writenewline
  #else do not write carriage return
  with open(SamplePostFilename, 'a') as outputfile:
     outputfile.write(writenewline)
     #outputfile.write("\n"+writenewline)
     return (newline)

#Simulate streaming data by generating and outputing a simulated social media post
def SimulatedStreamLine(PColl, SamplePostFilename):
   SampleDataGenerator = (PColl
      | 'Parse' >> beam.Map(ParseFile)
      | 'RandSent' >> beam.Map(RandomSentence, 50, 144) #,maxWords,truncate
      | 'RandPost' >> beam.Map(RandomPost)
      #'Display' being used to see current line for troubleshooting prior to write/append
      | 'Display' >> beam.Map(Disp)   
      | 'Append' >> beam.Map(AppendOutputFile, SamplePostFilename)
      )


