import requests as rq
import bs4 as bs
import re
from re import search
import time
import pandas as pd
from datetime import datetime
import os
import numpy as np
from multiprocessing import  Pool, cpu_count
import json

import nltk
import string
from nltk.tokenize import word_tokenize
from nltk.tokenize import RegexpTokenizer

from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
nltk.download('stopwords')
nltk.download('punkt')

import plotly.express as plx


def parallel_html_downloader(links:list,directory="downloads"):
    if not os.path.exists(directory):
        os.makedirs(directory)
    """
    Parallelize html downloading by the means of multithreading
    """
    def downloader(url:str, directory="downloads"):
        try:
            raw = rq.get(url, headers={'User-Agent':random.choice(headers)})   # pick a random user agent for the request
            while raw.status_code != 200:
                #print(f"Key error: {raw.status_code}. Sleeping 180s...")    # wait 180 seconds before doing another get (too many requests)
                time.sleep(180)
                raw = rq.get(url, headers={'User-Agent':random.choice(headers)})

            with open(f"{directory}/{url.split('/')[-1]}.html","w") as f:    # save the file.html in the folder
                f.write(raw.text) 
        except Exception as e:
            print(e)
            
    with ThreadPoolExecutor() as executor:
        executor.map(downloader, links)    # parallelize the downloader function over list of links

def extract_single_place(page):
    """
    Extract the requested features from a local html page
    """
    
    # Read local files
    with open(page,encoding="utf-8") as f:
        soup = bs.BeautifulSoup(f)
    
    placeName = soup.find_all('h1', {'class':'DDPage__header-title'})[0].contents[0]
    
    placeTags = list()
    for tags in soup.find_all('a', {'class':'itemTags__link js-item-tags-link'}):
        wordlen=len(tags.text)-2
        tag = tags.text[1:wordlen]
        placeTags.append(str(tag))
    
    been = soup.find_all('div', {'class':'col-xs-4X js-submit-wrap js-been-to-top-wrap action-btn-col hidden-print'})[0]
    num_been = been.get_text().split()
    numPeopleVisited = int(num_been[2])
    if numPeopleVisited==0:
        numPeopleVisited = ''
    
    want = soup.find_all('div', {'class':'col-xs-4X js-submit-wrap js-like-top-wrap action-btn-col hidden-print'})[0]
    num_want = want.get_text().split()
    numPeopleWant = int(num_want[3])
    if numPeopleWant==0:
        numPeopleWant = ''
    
    description = soup.find('div', class_='DDP__body-copy')
    allowlist = ['p', 'span', 'a', 'i']
    text_elements = [t for t in description.find_all(text=True) if t.parent.name in allowlist]
    placeDesc = str(' '.join(text_elements))
    placeDesc = placeDesc.replace(u'\xa0',u' ')
    
    
    placeShortDesc = soup.find_all('h3', {'class':'DDPage__header-dek'})[0].contents[0]
    placeShortDesc = placeShortDesc.replace(u'\xa0',u' ')
    placeShortDesc = str(placeShortDesc)
    
    placeNearby=list()
    for places in soup.find_all('div', {'class':'DDPageSiderailRecirc__item-title'}):
        placeNearby.append(str(places.text))
    if len(placeNearby) == 0:
        placeNearby = ''
    
    
    placeRaw= soup.find_all('address', class_='DDPageSiderail__address')[0]
    place = placeRaw.find_all('div')[0].contents[0:5:2]
    place = " ".join(place)
    placeAddress = place.replace('\n', '')
    
    
    coordinates = soup.find_all('div', class_='DDPageSiderail__coordinates')[0]
    coordinates = coordinates.get_text().split()
    Alt = coordinates[0]
    Altlen = len(Alt)
    placeAlt = float(Alt[0:Altlen-1])
    placeLong = float(coordinates[1])
    

    editors = soup.find_all('li', {'class':'DDPContributorsList__item'})
    if len(editors)==0:
        #placeEditors = soup.find_all('div', {'class':'DDPContributorsList'})[1].get_text().split()
        #TODO: check the line below
        listEditor = soup.find_all('div', {'class':'DDPContributorsList'})
        if len(listEditor) == 0:
            placeEditors=[""]
        else:
            placeEditors = listEditor[0].get_text().split()
    else:
        placeEditors = list()
        for place in editors:
            names = place.find('span').getText()
            placeEditors.append(names)
    
    
    date_time = soup.find_all('div', {'class':'DDPContributor__name'})
    if len(date_time) >0:
        time = date_time[0].get_text()
        placePubDate = datetime.strptime(time, '%B %d, %Y')
    else:
        placePubDate = ""
    
    
    titles = soup.find_all('h3', class_='Card__heading --content-card-v2-title js-title-content')  
    placeRelatedPlaces = list()
    for title in titles:
        big_check = title.parent.parent.parent.parent.parent.parent
        check = big_check.find('div', class_="CardRecircSection__title").get_text()
        if check == 'Related Places':
            placeRelatedPlaces.append(str(title.get_text().strip()))
    
    placeRelatedLists = list()
    for title in titles:
        big_check = title.parent.parent.parent.parent.parent.parent
        check = big_check.find('div', class_="CardRecircSection__title").get_text()
        if search("Appears in", check):
            placeRelatedLists.append(str(title.get_text().strip()))
    if len(placeRelatedLists)==0:
        placeRelatedLists.append('')
    
    find_url = soup.find('link', {"rel": "canonical"})
    placeURL = find_url['href']

    
    return {'placeName': placeName,
            'placeTags': str(placeTags),
            'numPeopleVisited': numPeopleVisited,
            'numPeopleWant': numPeopleWant,
            'placeDesc': placeDesc,
            'placeShortDesc':placeShortDesc,
            'placeNearby':str(placeNearby),
            'placeAddress': placeAddress,
            'placeAlt': placeAlt,
            'placeLong': placeLong,
            'placeEditors': str(placeEditors),
            'placePubDate': placePubDate,
            'placeRelatedPlaces': str(placeRelatedPlaces),
            'placeRelatedLists': str(placeRelatedLists),
            'placeURL': placeURL}


def text_cleaner_2(description):
    """
    stemming function
    """
    if type(description) != str:
        return None
    
    stop_words = set(stopwords.words('english'))
    snow_stemmer = SnowballStemmer(language='english')
    punct =  set(string.punctuation)
    punct.add("“")
    punct.add("”")
    punct.add("’")

    filtered_sentence = []
    word_tokens = word_tokenize(description)
    for w in word_tokens:
        if w not in stop_words :
            filtered_sentence.append(w)

    stemmed_desc = []
    for w in filtered_sentence:
        x = snow_stemmer.stem(w)
        stemmed_desc.append(x)

    filtered_desc = []
    for s in stemmed_desc:
        if s not in punct:
            filtered_desc.append(s)

    return filtered_desc


def vocabulary_maker(s:set, name="vocabulary.json"):
    """
    assign a number to each word in the set and create the vocabulary file (JSON format)
    """
    d, i = {}, 0
    for w in s:
        d[w] = i
        i += 1
    with open(name, "w") as f:
        f.write(json.dumps(d))


def query_function(query, voc="vocabulary.json", reverse_index="reverse_index.json", df_name="res.tsv"):
    df = pd.read_csv(df_name, delimiter = '\t')    # load the dataset
    with open(reverse_index, "r") as f:           # load the reverse index
        reverse_index = json.load(f)
    with open(voc, "r") as f:                   # load the vocabulary
        vocabulary = json.load(f)
    
    idx = vocabulary[query.split()[-1]]                      # find the id of the first word in the vocabulary
    s = set(reverse_index[str(idx)])                         # create a set with the documents related to the first word
    for w in query.split()[:-1]:
        idx = vocabulary[w]                                  # find the id for the remainig words
        s.intersection(set(reverse_index[str(idx)]))         # perform the intersection on the sets of documents
    return df.iloc[list(s)][["placeName","placeDesc","placeURL"]]


def cosine_similarity(v1,v2):
    """
    copmute the cosine similarity between the query and the documents
    """
    return np.dot(v1,v2)/(np.linalg.norm(v1)*np.linalg.norm(v2))




def query_function_v2(query, voc="vocabulary.json", reverse_index="reverse_index_v2.json", df_name="res.tsv"):
    df = pd.read_csv(df_name, delimiter = '\t')
    with open(reverse_index, "r") as f:
        reverse_index = json.load(f)
    with open(voc, "r") as f:
        vocabulary = json.load(f)
    
    stemmed_query = text_cleaner_2(query)                                     # stemming of the query
    token_query = list(map(lambda w: vocabulary[w],stemmed_query ))           # tokenization
    
    filtered_dict = { token: reverse_index[str(token)] for token in token_query }    # reduce the index only on the words in the query

    tfidf_vec = list(map(lambda w: token_query.count(w)/len(token_query) * np.log(len(df)/len(filtered_dict[w])), token_query))     # tfidf vector of the  query
    
    
    docs = set([tup[0] for l in filtered_dict.values() for tup in l ])    # extract all the documents
    
    
    tfidf_docs = {}
    for doc in docs:
        v=[]
        for word in token_query:
            for t in reverse_index[str(word)]:      # craete the tf-idf vector for each document
                if t[0]==doc:
                    v.append(t[1])
                    break
            else:
                v.append(0)
        tfidf_docs[doc] = v
    
    res = df.filter(items=list(docs), axis=0)
    res["Cosine similarity"] = res.index.map(lambda x: cosine_similarity(tfidf_vec,tfidf_docs[x] ))     # add the cosine similarity score
    
    return res[["placeName","placeDesc","placeURL","Cosine similarity"]].sort_values("Cosine similarity",ascending=False,kind="heapsort")


def query_function_v3(query, voc="vocabulary_q3.json", reverse_index="reverse_index_v3.json", df_name="res.tsv"):
    """
    Same as query_function_v2 but it implement the functionality to exclude terms from the query
    """
    df = pd.read_csv(df_name, delimiter = '\t')
    with open(reverse_index, "r") as f:
        reverse_index = json.load(f)
    with open(voc, "r") as f:
        vocabulary = json.load(f)
        
    neg_tf = 0
    if len(query.split(" -")) > 1 :                        # compute the negative signs before stemming the query
        neg_tf = len(query.split(" -")[-1].split())
    
    stemmed_query = text_cleaner_2(query)
    token_query = list(map(lambda w: vocabulary[w],stemmed_query ))

    filtered_dict = { token: reverse_index[str(token)] for token in token_query }

    tfidf_vec = list(map(lambda w: token_query.count(w)/len(token_query) * np.log(len(df)/len(filtered_dict[w])), token_query))
    
    for i in range(1,neg_tf+1):                            # assign the negative sign to the tf-idf vector
        tfidf_vec[-i] = -tfidf_vec[-i]
    
    docs = set([tup[0] for l in filtered_dict.values() for tup in l ])
    
    
    tfidf_docs = {}
    for doc in docs:
        v=[]
        for word in token_query:
            for t in reverse_index[str(word)]:
                if t[0]==doc:
                    v.append(t[1])
                    break
            else:
                v.append(0)
        tfidf_docs[doc] = v
    
    res = df.filter(items=list(docs), axis=0)
    res["Cosine similarity"] = res.index.map(lambda x: cosine_similarity(tfidf_vec,tfidf_docs[x] ))
    
    return res[["placeName","placeDesc","placeURL","Cosine similarity"]].sort_values("Cosine similarity",ascending=False,kind="heapsort")


def extract_country(row):
    raw = row.split(",")[-1]
    country = ''.join(c for c in raw if c.isalpha() or c == " ")
    return text_cleaner_2(country)


def query_function_v3_map(query, voc="vocabulary_q3.json", reverse_index="reverse_index_v3.json", df_name="res.tsv"):
    """
    Same as query_function_v3 but it gives back all the dataset
    """
    df = pd.read_csv(df_name, delimiter = '\t')
    with open(reverse_index, "r") as f:
        reverse_index = json.load(f)
    with open(voc, "r") as f:
        vocabulary = json.load(f)
        
    neg_tf = 0
    if len(query.split(" -")) > 1 :                        # compute the negative signs before stemming the query
        neg_tf = len(query.split(" -")[-1].split())
    
    stemmed_query = text_cleaner_2(query)
    token_query = list(map(lambda w: vocabulary[w],stemmed_query ))

    filtered_dict = { token: reverse_index[str(token)] for token in token_query }

    tfidf_vec = list(map(lambda w: token_query.count(w)/len(token_query) * np.log(len(df)/len(filtered_dict[w])), token_query))
    
    for i in range(1,neg_tf+1):                            # assign the negative sign to the tf-idf vector
        tfidf_vec[-i] = -tfidf_vec[-i]
    
    docs = set([tup[0] for l in filtered_dict.values() for tup in l ])
    
    
    tfidf_docs = {}
    for doc in docs:
        v=[]
        for word in token_query:
            for t in reverse_index[str(word)]:
                if t[0]==doc:
                    v.append(t[1])
                    break
            else:
                v.append(0)
        tfidf_docs[doc] = v
    
    res = df.filter(items=list(docs), axis=0)
    res["Cosine similarity"] = res.index.map(lambda x: cosine_similarity(tfidf_vec,tfidf_docs[x] ))
    
    res = res.sort_values("Cosine similarity",ascending=False,kind="heapsort")

    plot_map = plx.scatter_mapbox(res, lat = "placeAlt", lon = "placeLong",  width = 1000, opacity=1,  
    height = 600, size="numPeopleVisited", color="placeName", zoom=2,
    hover_name="placeName", hover_data = ["placeName", "placeAddress", "numPeopleVisited"])
    plot_map.update_layout(mapbox_style="open-street-map") 
    plot_map.show() 
    
    return

