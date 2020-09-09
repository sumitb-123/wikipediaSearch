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

def removeStopWords(data):
    stop_words = set(stopwords.words('english'))
    newStopWords = ['a','b','c','d','e','f','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z', 'ref','cite','0','1','2','3','4','5','6','7','8','9']
    stop_words.update(newStopWords)
    return [word for word in data if word not in stop_words]

def stemming(text):
    stemmer = SnowballStemmer("english")
    text = text.split()
    stemmed_words = [stemmer.stem(word) for word in text]
    return stemmed_words

def generateTokens(data):
    global no_of_tokens
    global doc_tokens
    data = data.encode('ascii', errors='ignore').decode()
    data = re.sub(r'[^A-Za-z0-9]+', r' ', data)
    tokens = nltk.word_tokenize(data)
    no_of_tokens += len(tokens)
    doc_tokens   += len(tokens)
    return tokens

def textHandler(text):
    stop_words = {}
    tokens = generateTokens(text)
    uwords = removeStopWords(tokens)
    swords = stemming(' '.join(uwords))
    return swords

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
    ext  = frequecyCounter(ext)
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
    global doc_id_title
    global doc_count
    doc_count += 1
    #print(doc_count)
    doc_id_title[id] = title
    #processing the title
    field_dict["title"]      = frequecyCounter(textHandler(title))
    field_dict["links"]      = separateExtLinks(text)
    field_dict["category"]   = seprateCategories(text)
    field_dict["infobox"]    = separateInfobox(text)
    field_dict["body"]       = separateBody(text)
    field_dict["ref"]        = separateReferences(text)
    field_dict["id"]         = id
    return field_dict

def updatePostList(field_data, field, field_id, doc_title):
    global PostList
    #print(field_data[field])
    weight = {"t":16, "i": 8, "c": 4, "b":2, "e":1, "r":1, "l":1}
    for key in field_data[field].keys():
        if field_id not in PostList[key].keys():
           PostList[key][field_id] = []
        #PostList[key][field_id].append((field_data["id"], field_data[field][key]))
        tf = float("{:.3f}".format(weight[field_id]*math.log(1 + field_data[field][key]/float(len(field_data[field])))))
        PostList[key][field_id].append((field_data["id"], tf))
        pass

def indexCreationParallel(field_data, title):
    #PostList["key"] = {"t":[(1,10),(2,10),...],"b":[(),(),..],"i":[(),(),..],"c":[(),(),..],"l":[(),(),..]}
    #id is the id of page i.e. title
    title_thread  = threading.Thread(target=updatePostList, args=(field_data,"title","t",title,))
    body_thread   = threading.Thread(target=updatePostList, args=(field_data,"body","b",title,))
    ref_thread    = threading.Thread(target=updatePostList, args=(field_data,"ref","r",title,))
    links_thread  = threading.Thread(target=updatePostList, args=(field_data,"links","l",title,))
    cat_thread    = threading.Thread(target=updatePostList, args=(field_data,"category","c",title,))
    in_thread     = threading.Thread(target=updatePostList, args=(field_data,"infobox","i",title,))

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

# no. of tokens before and after Indexing
def indexStat(path, index_token_size, xml_token_size):
    stats = str(xml_token_size)+"\n"+str(index_token_size)
    f = open(path, "w")
    f.write(stats)
    f.close()

#list all files in a directory
def listAllFiles(dirname):
    dirlist = os.listdir(dirname)
    return dirlist

# write Index to file in dict format
def writeIndexToFile(index_path):
    global PostList
    word_info = ""
    f = open(index_path,"w")
    for e in sorted(PostList.keys()):
        word_info = "{'"+e+"':"+str(PostList[e])+"}\n"
        f.write(word_info)
    f.close()
    pass

# word is present no. of documents
def wordFreqDoc(field_data):
    global PostList
    fields = ["title","infobox","category","body","links","ref"]
    words = []
    for ele in fields:
        words.extend(field_data[ele].keys())
    words = set(words)
    for key in words:
        if "fq" not in PostList[key].keys():
            PostList[key]["fq"] = 1
        else:
            PostList[key]["fq"] += 1

#Merging the partial index(sorted) files using k way merge
def mergeIndex(partial_index_path):
    global doc_count
    lst = listAllFiles(partial_index_path)
    file_handler = []
    exhausted    = [False for i in range(len(lst))]
    handler_size = len(lst)
    fhandle = "file"
    for i in range(handler_size):
        fp = fhandle + str(i)
        fp = open(partial_index_path + lst[i], "r")
        file_handler.append(fp)
    #taking first key of all index files
    word_info = []
    for handle in file_handler:
        word = handle.readline()
        word = word.strip()
        word_info.append(eval(word))
    #putiing all keys and index(file handle) into heap
    heap = [(list(word_info[e].keys())[0],e) for e in range(len(word_info))]
    heapq.heapify(heap)
    opfhandle = open(partial_index_path+"merged_index", "w")
    offsethandle = open(partial_index_path+"offset_index", "w")
    offset = 0
    pre = ""
    while len(heap) > 0:
        val = heapq.heappop(heap)
        if pre == "":
            pre = word_info[val[1]]
            temp_key = val[0]
            pass
        elif  list(pre.keys())[0] != val[0]:
            if offset % 10 == 0:
                offsethandle.write("{"+val[0] +":"+ str(offset) +"}\n")
            pre[temp_key]["idf"] = math.log(doc_count / pre[temp_key]["fq"])
            opfhandle.write(str(pre)+"\n")
            pre = word_info[val[1]]
            temp_key = val[0]
            offset += 1
        else:
            for el in word_info[val[1]][val[0]].keys():
                if el == "fq":
                   pre[val[0]]["fq"] += word_info[val[1]][val[0]]["fq"]
                else:
                    if el in pre[val[0]].keys():
                        pre[val[0]][el].extend(word_info[val[1]][val[0]][el])
                    else:
                        pre[val[0]][el] = word_info[val[1]][val[0]][el]
        word = file_handler[val[1]].readline()
        if word != "":
           new_word = eval(word)
           word_info[val[1]] = new_word
           heap.append((list(new_word.keys())[0], val[1]))
        heapq.heapify(heap)
    if offset % 10 != 0:
        offsethandle.write("{"+val[0] +":"+ str(offset) +"}\n")
    opfhandle.write(str(pre)+"\n")

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
      indexCreationParallel(field_data, self.Title)
      wordFreqDoc(field_data)
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

def parserHandler(xml_file_path, index_target_path):
    parser = xml.sax.make_parser()
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
    handler = XMLHelper()
    parser.setContentHandler(handler)
    parser.parse(open(xml_file_path))
    writeIndexToFile(index_target_path)

if  __name__ == '__main__':
    #counter starts
    start_time   = time.process_time()
    xml_dir_path   = sys.argv[1]
    index_dir_path = sys.argv[2]
    metadata_path  = sys.argv[3]
    doc_id_title   = {}
    token_doc_freq = {}
    all_words      = {}
    no_of_tokens   = 0
    doc_tokens     = 0
    doc_count      = 0
    thread_count   = 0
    PostList       = defaultdict(dict)
    #getting list of all files
    xml_file_list  = listAllFiles(xml_dir_path)
    while len(xml_file_list) > 0:
        xml_file = xml_file_list.pop()
        thread_count += 1
        parse_thread  = threading.Thread(target=parserHandler, args=(xml_dir_path + xml_file, index_dir_path+"index"+str(thread_count)))
        parse_thread.start()
        parse_thread.join()
        doc_tokens = 0
        PostList  = defaultdict(dict)
    print(doc_count)
    f = open(metadata_path + "doc_count", "w")
    f.write(str(doc_count))
    f.close()
    pickleDumpFile(metadata_path+"metadata", doc_id_title)
    mergeIndex(index_dir_path)
    print(time.process_time() - start_time)
