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
    pages = {}
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
                if e[0] in pages.keys():
                    pages[e[0]] += e[1]*idf
                else:
                    pages[e[0]] = e[1]*idf
    return pages

#If no docs are relevant to search 
def mostSearchedDocs():
    print("Most searched results ")
    global start_time
    global title
    global k
    count = 0
    with open(output_path+"search_results","a") as handle:
        for e in title.keys():
            handle.write(e+" "+title[e] + "\n")
            count += 1
            if count == k:
                break
        process_time = time.process_time() - start_time
        start_time   = time.process_time()
        handle.write(str(process_time)+"\n\n")
    pass

#list of query and path of all index files
def nonFieldQuery(query, path, output_path):
    global index_line_count
    global offset
    global k
    global title
    results = {}
    ranks = []
    for q in query:
        if q in offset.keys():
            offset_no = offset[q]
        else:
            continue
        #offset no. in the actual file
        if offset_no % index_line_count == 0:
           loff = int(offset_no / index_line_count)
        else:
           loff = int(offset_no / index_line_count) + 1
        plist = {}
        #print(loff)
        ifile = str(loff)
        #print(q)
        with open(path+ifile, 'rb') as handle:
            postlist = pickle.load(handle)
            #print(postlist)
            plist[q] = postlist[q]
        page_ranks = pageRanking(plist[q], "")
        for pid in page_ranks.keys():
            if pid in results.keys():
                results[pid] += page_ranks[pid]
            else:
               results[pid]   = page_ranks[pid]
    
    for ids in results.keys():
        ranks.append((results[ids], ids))
    
    if len(ranks) > 0:
        sorted(ranks, reverse=True)
    else:
        mostSearchedDocs()
   
    global start_time
    nk = min(k, len(ranks))
    with open(output_path+"search_results","a") as handle:
        for i in range(nk):
            handle.write(ranks[i][1]+" "+title[ranks[i][1]] + "\n")
        process_time = time.process_time() - start_time
        start_time   = time.process_time()
        handle.write(str(process_time)+"\n\n")

def fieldQuery(field_query_tuple, path, output_path):
    global index_line_count
    global offset
    global k
    global title
    all_query = []
    results = {}
    ranks = []
    for q in field_query_tuple:
        fld = q[0]
        qlist = q[1]
        for eq in qlist:
            all_query.append(eq)
            if eq in offset.keys():
                offset_no = offset[eq]
            else:
                continue
            #offset no. in the actual file
            if offset_no % index_line_count == 0:
                loff = int(offset_no / index_line_count)
            else:
                loff = int(offset_no / index_line_count) + 1
            plist = {}
            ifile = str(loff)
            with open(path+ifile, 'rb') as handle:
                postlist = pickle.load(handle)
                plist[eq] = postlist[eq]
            page_ranks = pageRanking(plist[eq], fld)
            for pid in page_ranks.keys():
                if pid in results.keys():
                    results[pid] += page_ranks[pid]
                else:
                    results[pid]   = page_ranks[pid]

    for ids in results.keys():
        ranks.append((results[ids], ids))

    if len(ranks) > 0:
        sorted(ranks, reverse=True)
        nk = min(k, len(ranks))
        global start_time
        with open(output_path+"search_results","a") as handle:
            for i in range(nk):
                handle.write(ranks[i][1]+" "+title[ranks[i][1]] + "\n")
            process_time = time.process_time() - start_time
            start_time   = time.process_time()
            handle.write(str(process_time)+"\n\n")
    else:
        nonFieldQuery(all_query, path, output_path)

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

if __name__ == '__main__':
  #start_time   = time.process_time()
  index_path  = sys.argv[1]
  offset_path = sys.argv[2]+"offset_index"
  title_path  = sys.argv[3]+"id_title_map"
  query_path  = sys.argv[4]
  output_path = sys.argv[5]
  index_line_count = 200000
  k = 10
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
  
  start_time   = time.process_time()

  for q in query:
      fquery = 1
      si = -1
      ed = -1
      fields = ['t:', 'l:', 'c:','r:','b:','i:']
      tag = ""
      field_query = {'t':"",'b':"", 'c':"", 'i':"", 'r':"", 'l':""}
      lst = q.split(':')
      output = []
      if len(lst) == 1:
         fquery = 0
         query_lst = textHandler(q)
         nonFieldQuery(query_lst, index_path, output_path)
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
      if fquery == 1:
         qry = []
         for keys in field_query.keys():
             if field_query[keys] != "":
                qlist = textHandler(field_query[keys])
                qry.append((keys,qlist))
         fieldQuery(qry, index_path, output_path)
  print(time.process_time() - start_time)
