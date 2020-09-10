import nltk
#nltk.download('punkt')
#nltk.download('stopwords')
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
import os
import queue
import math
import pprint
import heapq

def removeStopWords(data):
    stop_words = set(stopwords.words('english'))
    newStopWords = ['a','b','c','d','e','f','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z', 'ref','cite','0','1','2','3','4','5','6','7','8','9']
    stop_words.update(newStopWords)
    return [word for word in data if word not in stop_words]

def stemming(text):
    stemmer = SnowballStemmer("english")
    text = text.split()
    stemmed_words = [stemmer.stem(word) for word in text if len(word) < 30]
    return stemmed_words

def generateTokens(data):
    data = data.encode('ascii', errors='ignore').decode()
    data = re.sub(r'[^A-Za-z0-9]+', r' ', data)
    tokens = nltk.word_tokenize(data)
    return tokens

def textHandler(text):
    tokens = generateTokens(text)
    uwords = removeStopWords(tokens)
    swords = stemming(' '.join(uwords))
    return swords

def pageRanking(plist, field):
    global title
    lst = []
    page = {}
    plist.pop("fq")
    idf  = plist["idf"]
    plist.pop("idf")
    fields = []
    if field == "":
        fields = ['t','b','r','l','c','i']
    else:
        fields.append(field)
    for f in fields:
        if f in plist.keys():
            for e in plist[f]:
                if e[0] in page.keys():
                    page[e[0]] = max(e[1]*idf,page[e[0]])
                else:
                    page[e[0]] = e[1]*idf
    
    for e in page.keys():
        lst.append((page[e],e))
    
    sorted(lst, reverse=True)
    doc = [(l[1],title[l[1]]) for l in lst]
    return doc

#list of query and path of all index files
def nonFieldQuery(query, path):
    global offset
    global k
    results = []
    for q in query:
        if q in offset.keys():
            offset_no = offset[q]
        else:
            continue
        #offset no. in the actual file
        loff      = offset_no
        plist = {}
        ifile = "merged_index"
        with open(path+ifile) as f:
            for i, line in enumerate(f):
                if i == loff:
                  plist = eval(line )
        results.extend(pageRanking(plist[q], ""))
    if len(results) > k:
        return results[:k]
    else:
        return results

def fieldQuery(query, path, field):
    global offset
    global k
    results = []
    for q in query:
        if q in offset.keys():
            offset_no = offset[q]
        else:
            continue
        #offset no. in the actual file
        loff  = offset_no
        plist = {}
        ifile = "merged_index"
        with open(path+ifile) as f:
            for i, line in enumerate(f):
                if i == loff:
                  plist = eval(line )
        results.extend(pageRanking(plist[q], field))

    if len(results) == 0:
        return nonFieldQuery(query, path)
    if len(results) > k:
        return results[:k]
    else:
        return results

def getQueryList(path):
    fp = open(path,"r")
    query = []
    for line in fp:
        temp = line.strip().split(',')
        query.append((int(temp[0]),temp[1].lower())) 
    return query

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

def writeResultTOFile(results):
    pass

if __name__ == '__main__':
  start_time   = time.process_time()
  index_path  = sys.argv[1]
  offset_path = sys.argv[2]+"offset_index"
  title_path  = sys.argv[3]+"id_title_map"
  query_path  = sys.argv[4]
  k = 100
  offset = ""
  #reading offset mapping
  with open(offset_path, 'rb') as handle:
      offset = pickle.load(handle)

  title = ""
  #reading id to title mapping
  with open(title_path, 'rb') as handle:
      title = pickle.load(handle)

  qry_lst = getQueryList(query_path)
  query = [x[1] for x in qry_lst]

  for q in query:
      fquery = 0
      si = -1
      ed = -1
      fields = ['t:', 'l:', 'c:','r:','b:','i:']
      tag = ""
      field_query = {'t':"",'b':"", 'c':"", 'i':"", 'r':"", 'l':""}
      lst = q.split(':')
      output = []
      if len(lst) == 1:
         fquery = 1
         query_lst = textHandler(q)
         output.extend(nonFieldQuery(query_lst, index_path))
         print(output)
      else:
          for f in fields:
              si = q.find(f)
              if si != -1:
                  ed = q.find(":",si+2)
                  if ed != -1:
                     tag = q[si+2:ed-1]
                  else:
                     tag = q[si+2::]
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
      output = []
      if fquery == 0:
         for keys in field_query.keys():
             if field_query[keys] != "":
                qlist = textHandler(field_query[keys])
                output.extend(fieldQuery(qlist, index_path, keys))
         print(output)
  print(time.process_time() - start_time)
