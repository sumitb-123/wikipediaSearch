import nltk
from xml.sax.handler import ContentHandler
from nltk.stem.snowball import SnowballStemmer
from collections import defaultdict
from nltk.corpus import stopwords
import xml.sax
import re
import json
import time
import threading
import _pickle as pickle
import gc
import sys

def removeStopWords(data, stop_words):
    stop_words = set(stopwords.words('english'))
    return [word for word in data if word not in stop_words]

def stemming(text):
    stemmer = SnowballStemmer("english", ignore_stopwords=True)
    return stemmer.stem(text)

def generateTokens(data):
    data = data.encode('ascii', errors='ignore').decode()
    data = re.sub(r'[^A-Za-z0-9]+', r' ', data)
    tokens = nltk.word_tokenize(data)
    return tokens

def textHandler(text):
    stop_words = {}
    tokens = generateTokens(text)
    uwords = removeStopWords(tokens, stop_words)
    #swords = stemming(uwords)
    return uwords

def searchQueryPickle(path, field, word):
    with open(path, 'rb') as handle:
      postlist = pickle.load(handle)
    if len(field) == 0:
       if word in postlist.keys():
          print(postlist[word])
    else:
       if field not in postlist[word].keys():
          pass
       else:
          print(postlist[word][field])

if __name__ == '__main__':
  no_of_args = len(sys.argv)
  index_path = sys.argv[1]+"invertedPkl"
  
  query = ""
  if no_of_args > 3:
    for i in range(2,no_of_args):
        print(sys.argv[i])
        query += sys.argv[i]+" "
  else:
      query = sys.argv[2]
  
  query = query.lower()

  si = -1
  ed = -1

  print("output format: (doc_id, count)")
  print("t: title, i: infobox, l:external links, b:body, c:category, r:references")
  print("field query o/p doc id and frequency of searched word in the given field")
  print("no output means no match")

  fields = ['t:', 'l:', 'c:','r:','b:','i:']
  tag = ""
  field_query = {'t':"",'b':"", 'c':"", 'i':"", 'r':"", 'l':""}
  lst = query.split(':')
  if len(lst) == 1:
     query_lst = textHandler(query)
     for q in query_lst:
         searchQueryPickle(index_path,"",q)
  else:
      for f in fields:
          si = query.find(f)
          if si != -1:
            ed = query.find(":",si+2)
            if ed != -1:
               tag = query[si+2:ed-1]
            else:
               tag = query[si+2::]
            if f == "t:":
                field_query['t'] += tag
            elif f == "c:":
                field_query['c'] += tag
            elif f == "l:":
                field_query['l'] += tag
            elif f == "r:":
                field_query['r'] += tag
            elif f == "b:":
                field_query['b'] += tag
            elif f == "i:":
                field_query['i'] += tag
            tag = ""

  for keys in field_query.keys():
      if field_query[keys] != "":
         query_lst = textHandler(field_query[keys])
         for q in query_lst:
            searchQueryPickle(index_path,keys,q)





