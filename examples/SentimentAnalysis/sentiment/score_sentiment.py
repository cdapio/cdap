#!/usr/bin/env python

# Copyright 2013-2014 Continuuity, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

import sys
sys.path.append(".")

import re
import csv
import pprint
import nltk.classify
import pickle
import time

stopWords = []
featuresDict = {}

#---------------------------------------------------------------------------------
# Reads the tweets from standard input and trains the model.
#---------------------------------------------------------------------------------
f = open('data/naive-bayes.model.pickle')
classifier = pickle.load(f)
f.close()

#---------------------------------------------------------------------------------
# Pre-Processes a tweet
#---------------------------------------------------------------------------------
def process(tweet):
  # lower case the tweet.
  tweet = tweet.lower()

  # remove url. 
  tweet = re.sub('((http://[^\s]+)|(www\.[\s]+)|(https?://[^\s]+))','URL',tweet)

  # Convert user name to USER
  tweet = re.sub('@[^\s]+', 'USER', tweet)

  # Remove any additional space
  tweet = re.sub('[\s]+', ' ', tweet)

  # Replace #word with word
  tweet = re.sub(r'#([^\s]+)', r'\1', tweet)

  # trim
  tweet = tweet.strip('\'"')

  return tweet

#---------------------------------------------------------------------------------
# Loads the stop words from the file.
#---------------------------------------------------------------------------------
def load(filename):
  #read the stopwords
  words = []
  fp = open(filename, 'r')
  line = fp.readline()
  while line:
    word = line.strip()
    words.append(word)
    line = fp.readline()
  fp.close()
  return words

#---------------------------------------------------------------------------------
# Split the tweet into vector of words.
#---------------------------------------------------------------------------------
def vector(tweet, stopWords):
  pattern = re.compile(r"(.)\1{1,}", re.DOTALL)
  vector = []
  words = tweet.split()
  for w in words:
    w = pattern.sub(r"\1\1", w)
    w = w.strip('\'"?,.')
    val = re.search(r"^[a-zA-Z][a-zA-Z0-9]*[a-zA-Z]+[a-zA-Z0-9]*$", w)
    if not val or not w in stopWords:
      vector.append(w)
  return vector

#---------------------------------------------------------------------------------
# Convert the tweets into the fixed dimensions.
#---------------------------------------------------------------------------------
def dimension(tweet):
  tweet_words = set(tweet)
  features = dict(featuresDict)
  for word in tweet_words:
    features['contains(%s)' % word] = True
  return features

#---------------------------------------------------------------------------------
# Generates a sentiment for a tweet based on the trained model.
#---------------------------------------------------------------------------------
def sentiment(line):
  # remove '\n' from line.
  tweet = line.rstrip('\n')

  # process tweet.
  processedTweet = process(tweet)

  # Get vectors from processed tweet.
  v = vector(processedTweet, stopWords)

  # Add to training data
  sentiment = classifier.classify(dimension(v))

  return sentiment

  # Print sentiment.
  print "%s" % (sentiment)

def main():
  """ 
  Load stop words and feature dictionary. Translate the dictionary 
  into set.
  """
  stopWords = load('data/stopwords.txt');
  stopWords.append('USER')
  stopWords.append('URL')
  featureDict = load('data/features.txt')

  for word in featureDict:
    featuresDict['contains(%s)' % word] = False

  while True:
    #time.sleep(0.05)
    try:
      tweets = sys.stdin.readline()
    except KeyboardInterrupt:
      break

    sys.stderr.write("Got %s" % tweets)
    
    if not tweets:
      break

    sentiments = []
    tweet = tweets.split(':::')[0]
    s = sentiment(tweet)
    sentiments.append(tweet.rstrip())
    sentiments.append(s)
    print "%s" % '---'.join(str(x) for x in sentiments)
    sys.stdout.flush()

  """
  import cProfile
  cProfile.run('sentiment("i love movie")')
  """

if __name__ in "__main__":
  main()
