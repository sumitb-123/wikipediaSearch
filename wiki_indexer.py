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
import os

def frequecyCounter(words):
    global all_words
    freq = {}
    for word in words:
      if word in freq.keys():
        freq[word] += 1
      else:
        freq[word]  = 1
      if word in all_words.keys():
         all_words[word] += 1
      else:
         all_words[word] = 1
    return freq

def jsonDumpFile(dumpPath,data):
    json_object = json.dumps(data)
    with open(dumpPath, "w") as outfile: 
        outfile.write(json_object)
  
def pickleDumpFile(dumpPath,postlist):
    data = postlist
    with open(dumpPath, 'wb') as handle:
      pickle.dump(data, handle, protocol=-1)

def removeStopWords(data, stop_words):
    stop_words = set(stopwords.words('english'))
    return [word for word in data if word not in stop_words]

def stemming(text):
    stemmer = SnowballStemmer("english", ignore_stopwords=True)
    return stemmer.stem(text)

def generateTokens(data):
    global no_of_tokens
    data = data.encode('ascii', errors='ignore').decode()
    data = re.sub(r'[^A-Za-z0-9]+', r' ', data)
    tokens = nltk.word_tokenize(data)
    no_of_tokens += len(tokens)
    return tokens

def textHandler(text):
    stop_words = {}
    tokens = generateTokens(text)
    uwords = removeStopWords(tokens, stop_words)
    #swords = stemming(uwords)
    return uwords

def seprateCategories(text):
    ctry = []
    if len(text) == 0:
       return {}
    sz   = len("[[category:")
    patr = "[[category:"
    strt = 0
    end  = 0    
    txtlen = len(text)
    while strt < txtlen:
      strt = text.find(patr,strt)
      if strt == -1:
        break
      end  = text.find(']]',strt+1)
      if end == -1:
        break
      ctry.append(text[strt+sz:end])
      strt = end + 1
    text = ' '.join(ctry)
    ctry = textHandler(text)
    #print("category :",ctry)
    ctry = frequecyCounter(ctry)
    return ctry

def separateInfobox(data):
    if len(data) == 0:
       return {} 
    patr = "{{infobox"
    sz   = len(patr)
    strt = 0
    end  = 0
    infobox = ""
    strt = data.find(patr,strt)
    if strt == -1:
       return {}
    end  = data.find('}}',strt+1)
    if end == -1:
       end = len(data)
    infobox = data[strt+sz:end]
    infobox = textHandler(infobox)
    #print("infobox : ",infobox)
    infobox = frequecyCounter(infobox)
    return infobox

def separateReferences(text):
    if len(text) == 0:
       return {} 
    ref  = "== references =="
    strt = text.find(ref)
    if strt == -1:
       ref   = "==references=="
       strt = text.find(ref)
    if strt == -1:
       return {}
    sz   = len(ref)
    end  = text.find("== external links ==",strt+sz)
    if end == -1:
       end = text.find("==external links==",strt+sz)
    if end == -1:
       end = len(text)
    ref  = text[strt + sz:end]
    ref  = textHandler(ref)
    #print("ref : ",ref)
    ref = frequecyCounter(ref)
    return ref

def separateExtLinks(text):
    if len(text) == 0:
       return {} 
    ext   = "== external links =="
    strt  = text.find(ext)
    if strt == -1:
       ext   = "==external links=="
       strt = text.find(ext)
    if strt == -1:
       return {}
    sz   = len(ext)
    end  = text.find("[[category:",strt+sz)
    if end == -1:
       end = len(text)
    ext  = text[strt + sz:end]
    ext  = textHandler(ext)
    ext = frequecyCounter(ext)
    return ext

def separateBody(text):
    if len(text) == 0:
       return {}
    patr = "== references =="
    strt = text.find(patr)
    if strt == -1:
       patr   = "==references=="
       strt = text.find(patr)
    if strt == -1:
       patr   = "== external links =="
       strt = text.find(patr)
    if strt == -1:
       patr   = "==external links=="
       strt = text.find(patr)
    if strt != -1:
       text = text[0:strt]
    data = re.sub('\{\{v?cite[^}]*\}\}', '', text)
    data = re.sub(r'<ref.[^/>]*?/>', r'', data)
    data = re.sub(r'<ref(.|\n)*?/ref>', r' ', data)
    data = re.sub(r'\{\{infobox.[^}}]*\}\}', r' ', data)
    data = re.sub(r'\{\{.[^}}]*\}\}', r' ', data)
    data = re.sub(r'<!--.[^-->]*-->', r' ', data)
    #print("body : ",data)
    data = textHandler(data)
    data = frequecyCounter(data)
    return data

def textProcessHelper(title, text, id):
    #dict of all fields
    field_dict = {}
    title = title.lower()
    text  = text.lower()
    #processing the title
    field_dict["title"]      = frequecyCounter(textHandler(title))
    field_dict["links"]      = separateExtLinks(text)
    field_dict["category"]   = seprateCategories(text)
    field_dict["infobox"]    = separateInfobox(text)
    field_dict["body"]       = separateBody(text)
    field_dict["ref"]        = separateReferences(text)
    field_dict["id"]         = id
    return field_dict

def updatePostList(field_data, field, field_id):
    global PostList
    for title in field_data[field].keys():
        if field_id not in PostList[title].keys():
           PostList[title][field_id] = []
        PostList[title][field_id].append((field_data["id"],field_data[field][title]))
        pass

def indexCreationParallel(field_data):
    #PostList["key"] = {"t":[(1,10),(2,10),...],"b":[(),(),..],"i":[(),(),..],"c":[(),(),..],"l":[(),(),..]}
    #id is the id of page i.e. title
    title_thread  = threading.Thread(target=updatePostList, args=(field_data,"title","t",))
    body_thread   = threading.Thread(target=updatePostList, args=(field_data,"body","b",))
    ref_thread    = threading.Thread(target=updatePostList, args=(field_data,"ref","r",))
    links_thread  = threading.Thread(target=updatePostList, args=(field_data,"links","l",))
    cat_thread    = threading.Thread(target=updatePostList, args=(field_data,"category","c",))
    in_thread     = threading.Thread(target=updatePostList, args=(field_data,"infobox","i",))

    #starting the threads
    title_thread.start()
    body_thread.start()
    ref_thread.start()
    links_thread.start()
    cat_thread.start()
    in_thread.start()

    #joining the threads
    title_thread.join()
    body_thread.join()
    ref_thread.join()
    links_thread.join()
    cat_thread.join()
    in_thread.join()

def indexCreationWithKeys(field_data):
    global PostList
    #PostList["key"] = {"t":[(1,10),(2,10),...],"b":[(),(),..],"i":[(),(),..],"c":[(),(),..],"l":[(),(),..]}
    #id is the id of page i.e. title
    for title in field_data["title"].keys():
        if 't' not in PostList[title].keys():
           PostList[title]['t'] = []
        PostList[title]['t'].append((field_data["id"],field_data["title"][title]))
        
    for title in field_data["body"].keys():
        if 'b' not in PostList[title].keys():
           PostList[title]['b'] = []
        PostList[title]['b'].append((field_data["id"],field_data["body"][title]))
        
    for title in field_data["infobox"].keys():
        if 'i' not in PostList[title].keys():
           PostList[title]['i'] = []
        PostList[title]['i'].append((field_data["id"],field_data["infobox"][title]))
        
    for title in field_data["category"].keys():
        if 'c' not in PostList[title].keys():
           PostList[title]['c'] = []
        PostList[title]['c'].append((field_data["id"],field_data["category"][title]))
        
    for title in field_data["links"].keys():
        if 'l' not in PostList[title].keys():
           PostList[title]['l'] = []
        PostList[title]['l'].append((field_data["id"],field_data["links"][title]))
        
    #jsonDumpFile("/content/drive/My Drive/wiki_index.json",PostList)

def createIndex(field_data):
    #PostList["key"] = {"t":[(1,10),(2,10),...],"b":[(),(),..],"i":[(),(),..],"c":[(),(),..],"l":[(),(),..]}
    #id is the id of page i.e. title
    global all_words
    for word,count in all_words.items():
      if field_data["title"].get(word):
          if 't' not in PostList[word].keys():
             PostList[word]['t'] = []
          PostList[word]['t'].append((field_data["id"],field_data["title"][word]))

      if field_data["body"].get(word):
         if 'b' not in PostList[word].keys():
           PostList[word]['b'] = []
         PostList[word]['b'].append((field_data["id"],field_data["body"][word]))

      if field_data["infobox"].get(word):
         if 'i' not in PostList[word].keys():
           PostList[word]['i'] = []
         PostList[word]['i'].append((field_data["id"],field_data["infobox"][word]))

      if field_data["category"].get(word):
         if 'c' not in PostList[word].keys():
           PostList[word]['c'] = []
         PostList[word]['c'].append((field_data["id"],field_data["category"][word]))

      if field_data["links"].get(word):
         if 'l' not in PostList[word].keys():
           PostList[word]['l'] = []
         PostList[word]['l'].append((field_data["id"],field_data["links"][word]))

      if field_data["ref"].get(word):
         if 'r' not in PostList[word].keys():
           PostList[word]['r'] = []
         PostList[word]['r'].append((field_data["id"],field_data["ref"][word]))

def searchQueryJson(path, field, word):
    if len(field) == 0:
       pass
    fl = open(path)
    postlist = json.load(fl)
    if len(field) == 0:
       print(postlist[word])
    else:
       if field not in postlist[word].keys():
          print("sorry! could not find the word")
       else:
          print(postlist[word][field])

def searchQueryPickle(path, field, word):
    with open(path, 'rb') as handle:
      postlist = pickle.load(handle) 
    if len(field) == 0:
       print(postlist[word])
    else:
       if field not in postlist[word].keys():
          print("sorry! could not find the word")
       else:
          print(postlist[word][field])

def indexStat(path, index_token_size, xml_token_size):
    stats = str(xml_token_size)+"\n"+str(index_token_size)
    f = open(path, "w")
    f.write(stats)
    f.close()

class XMLHelper(xml.sax.handler.ContentHandler):
  def __init__(self):
    self.CurrentData = ""
    self.PageCount   = 0
    self.Text        = ""
    self.idCount     = 0
    self.titleCount  = 0
    self.ID          = ""
    self.Title       = ""

  def startElement(self, tag, attributes):
    self.CurrentData = tag
    if tag == "page":
      self.PageCount += 1
      self.idCount    = 0
      #print("page no. : ",self.PageCount)

  def endElement(self, tag):
    if self.CurrentData == "text":
      field_data = textProcessHelper(self.Title, self.Text, self.ID)
      indexCreationParallel(field_data)
      #indexCreation(field_data)
      #createIndex(field_data)
      self.Text        = ""
      self.CurrentData = ""
      self.idCount     = 0
      self.ID          = ""
      self.Title       = ""
      self.titleCount  = 0

  def characters(self, content):
    if self.CurrentData   == "title":
      if self.titleCount == 0:
         self.Title = content
      self.titleCount += 1
    elif self.CurrentData == "id":
      if self.idCount == 0:
          self.ID    = content
      self.idCount += 1
    elif self.CurrentData == "text":
      self.Text  += content

if __name__ == '__main__':
  no_of_args = len(sys.argv)
  xml_file_path = sys.argv[1]
  index_path    = sys.argv[2]+"invertedPkl"
  index_stat    = sys.argv[3]
  os.mkdir(sys.argv[2])
  start_time = time.process_time()
  parser = xml.sax.make_parser()
  parser.setFeature(xml.sax.handler.feature_namespaces, 0)
  handler = XMLHelper()
  parser.setContentHandler(handler)
  PostList  = defaultdict(dict)
  all_words = {}
  no_of_tokens = 0
  parser.parse(open(xml_file_path))
  gc.collect()
  pickleDumpFile(index_path, PostList)
  indexStat(index_stat, len(all_words.keys()), no_of_tokens)
  print(time.process_time() - start_time)
