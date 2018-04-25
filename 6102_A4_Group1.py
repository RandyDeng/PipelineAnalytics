import argparse
import apache_beam as beam
import itertools
import os
import csv
#import random
#import string
#import time
import A4ProjLib #Random Data Library for Assignment 4
#from A4ProjLib import RandAlphaNumeric
#from A4ProjLib import RandomDateTime
#from A4ProjLib import RandomSentence
#from A4ProjLib import RandomPost
from A4ProjLib import ParseFile
from A4ProjLib import Disp
#from A4ProjLib import AppendOutputFile
from A4ProjLib import SimulatedStreamLine
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

####################################################################################
# This funtion will generate an output file where each line is designed to simulate
#    an online post, with the data items in each line comma-delineated
def GenSampleData():
   with beam.Pipeline('DirectRunner') as dict1:
    with beam.Pipeline('DirectRunner') as dict2:
      dict_stndrd = dict1 | ReadFromText(Dictionary_Standard)
      dict_spec   = dict2 | ReadFromText(Dictionary_Special)
      PColl = dict_stndrd #combine with dict_spec
      
      #FAILED ATTEMPT TO CONCATENATE 2 PCOLLECTIONS
      #def ExpandDict(dictionary, appendix=""):
      #   if (appendix==""):
      #      return (dictionary)
      #   else:
      #      return (dict_stndrd)
      #
      #dictionary = dict_stndrd
      #PColl = (dictionary
      #   | 'Expand' >> beam.Map(ExpandDict, "cheese")
      #   )

      SimulatedStreamLine(PColl, SamplePostFilename)

def ReadSampleData():
   keys = []
   with beam.Pipeline('DirectRunner') as dict:
    with beam.Pipeline('DirectRunner') as samp:
      dict_spec = dict | ReadFromText(Dictionary_Special)
      samples   = samp | ReadFromText(SamplePostFilename)
      
      #def SplitIntoWords(line): 
      #   import re 
      #   return re.findall(r'[A-Za-z\']+', line)
      
      def CountOccurances(sample):#, keys=[]):
         testStr = sample[2]
         print ("In this sentence, Baymax appears:  << "+str(testStr.count('Baymax'))+" >> times!")
         return(sample)
         
      def GenKeyArray(k):
  #       print(k)
         for i in range (0,len(k)):
            keys.append(k[i])
            #print (k[i])
         print keys
         return(k)
      
      KeyWords = (dict_spec
         | 'ParseKeywords' >> beam.Map(ParseFile)
         | 'Convert2Array' >> beam.Map(GenKeyArray)
 #        | 'DiplayKeywords' >> beam.Map(Disp)
         )
      #keys = KeyWords
      print ("_____")
      print (keys)
 
      output = (samples
         | 'Parse' >> beam.Map(ParseFile)
         | 'Display' >> beam.Map(Disp)   
         | 'CountOcc' >> beam.Map(CountOccurances)#, [KeyWords])
         )
      #print output
     


####################################################################################
Dictionary_Standard = 'standard_dictionary.csv'
Dictionary_Special  = 'special_dictionary.csv'
SamplePostFilename = 'SimulatedPosts.csv'
GenerateSamples = False   #If false, will leave sample file alone
GenerateNumber = 30       #If generating samples, generate this many samples
overwrite = True          #If true, overwrite; if false, append
ReadSamples = True        #If true, read sample data

if GenerateSamples:       #Generate sample/simulated posts
   print ("...start writing...") #troubleshoot marker
   if overwrite:          #Replace simulated post data file
      if os.path.exists(SamplePostFilename):
        os.remove(SamplePostFilename)
        print("........old sample file removed")
   for i in range(GenerateNumber):
      GenSampleData()     #Generate a simulated post
   print ("...finish writing...") #troubleshoot marker

if ReadSamples:           #Read sample/simulated posts
   print ("...start reading...") #troubleshoot marker
   ReadSampleData()
   print ("...finish reading...") #troubleshoot marker
   


####################################################################################

