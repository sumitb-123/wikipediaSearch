import nltk
from xml.sax.handler import ContentHandler
from nltk.stem.snowball import SnowballStemmer
import xml.sax
import re

def frequecyCounter(words):
    freq = {}
    for word in words:
      if word in freq.keys():
        freq[word] += 1
      else:
        freq[word]  = 1
    return freq

def removeStopWords(data, stop_words):
    return [word for word in data if word not in stop_words.keys()]

def stemming(text):
    stemmer = SnowballStemmer("english", ignore_stopwords=True)
    return stemmer.stem(text)

def generateTokens(data):
    data = data.encode('ascii', errors='ignore').decode()
    data = re.sub(r'[^A-Za-z0-9]+', r' ', data)
    return nltk.word_tokenize(data)

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
       return []
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
       return []
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
       return []
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
    data = re.sub(r'\{\{.[^}}]*\}\}', r' ', text)
    data = textHandler(data)
    #print("body : ",data)
    data = frequecyCounter(data)
    return data

def textProcessHelper(title, text, id):
    #dict of all fields
    field_dict = {}
    title = title.lower()
    text  = text.lower()
    #processing the title
    field_dict["title"]      = textHandler(title)
    field_dict["links"]      = separateExtLinks(text)
    field_dict["categories"] = seprateCategories(text)
    field_dict["infobox"]    = separateInfobox(text)
    field_dict["body"]       = separateBody(text)
    field_dict["ref"]        = separateReferences(text)
    field_dict["id"]         = id
    """
    print("Title")
    print(field_dict["title"])
    print("categories")
    print(field_dict["categories"])
    print("Ext links") 
    print(field_dict["links"])
    print("infobox")
    print(field_dict["infobox"])
    print("body")
    print(field_dict["body"])
    print("references")
    print(field_dict["ref"])
    """
    return field_dict

def indexCreation(fields_data):    
    pass

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
      print("page no. : ",self.PageCount)

  def endElement(self, tag):
    if self.CurrentData == "text":
      fields = textProcessHelper(self.Title, self.Text, self.pageCount)
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
  parser = xml.sax.make_parser()
  parser.setFeature(xml.sax.handler.feature_namespaces, 0)
  handler = XMLHelper()
  parser.setContentHandler(handler)
  parser.parse(open('test.xml'))
