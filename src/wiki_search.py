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
    swords = list(set(stemming(' '.join(uwords))))
    return swords

def pageRanking(plist, field):
    global title
    lst = []
    pages = {}
    if "fq" in plist.keys():
        plist.pop("fq")
    idf = 1
    if "idf" in plist.keys():
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
def mostSearchedDocs(k):
    #print("Most searched results ")
    global start_time
    global title
    count = 0
    with open(output_path+"search_results","a") as handle:
        handle.seek(0, os.SEEK_END)
        for e in title.keys():
            handle.write(e+" "+title[e] + "\n")
            count += 1
            if count == k:
                break

#list of query and path of all index files
def nonFieldQuery(query, path, output_path):
    global index_line_count
    global offset
    global k
    global title
    global first
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
        ifile = str(loff)
        file_start_time = time.process_time()
        with open(path+ifile, 'rb') as handle:
            postlist = pickle.load(handle)
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
        ranks = sorted(ranks, reverse=True)
    else:
        mostSearchedDocs(k)
   
    global start_time
    nk = min(k, len(ranks))
    with open(output_path+"search_results","a") as handle:
        for i in range(nk):
            handle.write(ranks[i][1]+" "+title[ranks[i][1]] + "\n")
    if len(ranks) < k and nk > 0:
           mostSearchedDocs(k - len(ranks))
    process_time = time.process_time() - start_time
    start_time   = time.process_time()
    with open(output_path+"search_results","a") as handle:
        handle.write(str(process_time)+"\n\n")

def fieldQuery(field_query_tuple, path, output_path):
    global index_line_count
    global offset
    global k
    global title
    global first

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
            file_start_time = time.process_time()
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

    nk = 0
    if len(ranks) > 0:
        ranks = sorted(ranks, reverse=True)
        nk = min(k, len(ranks))
        global start_time
        with open(output_path+"search_results","a") as handle:
            for i in range(nk):
                handle.write(ranks[i][1]+" "+title[ranks[i][1]] + "\n")
    if nk < k:
        k = k - nk
        nonFieldQuery(all_query, path, output_path)
    else:
        process_time = time.process_time() - start_time
        start_time   = time.process_time()
        with open(output_path+"search_results","a") as handle:
            handle.write(str(process_time)+"\n\n")


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
  start_time   = time.process_time()
  index_path  = sys.argv[1]                     # path of inverted index file
  offset_path = sys.argv[2]+"offset_index"      # path of index offset file
  title_path  = sys.argv[3]+"id_title_map"      # path of id title meta file
  query_path  = sys.argv[4]                     # query file path
  output_path = sys.argv[5]                     # output dump path
  index_line_count = 10000
  # maximum no. of docs to show
  k = 100
  offset = ""
  
  # reading offset mapping
  with open(offset_path, 'rb') as handle:
      offset = pickle.load(handle)
  
  title = ""
  # reading id to title mapping
  with open(title_path, 'rb') as handle:
      title = pickle.load(handle)

  qry_lst = getQueryList(query_path)
  query  =  [x[1] for x in qry_lst]
  topdoc =  [x[0] for x in qry_lst] 
  start_time   = time.process_time()
  ind = 0
  for q in query:
      k    = topdoc[ind]
      ind += 1
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
