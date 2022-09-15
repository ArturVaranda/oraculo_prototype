# -*- coding: utf-8 -*-
"""
Created on Tue Dec 14 11:42:41 2021

@author: artur_varanda
"""
import re
import pandas as pd

from unidecode import unidecode
from nltk import WordNetLemmatizer
from sacremoses import MosesTokenizer, MosesDetokenizer
from nltk.tokenize import word_tokenize as tokenize

detokenizer = MosesDetokenizer(lang='en')
lemmatizer = WordNetLemmatizer()

def arg_cleaner(raw_string):
    new_arg_list = []
    if pd.isna(raw_string) == False:
        arg_el = str(raw_string).split(";")
        for el in arg_el:
            new_arg_list.append(el)
            
    return new_arg_list

def yaml_parentheses_cleaner(test_text):
    
    values = re.finditer("(\([^\(\)]*?\))(?!\[.*?\])",test_text)
    for value in values:
        no_p_value = re.sub("[\(\)]","",value[0])
        test_text = test_text.replace(value[0],no_p_value)

    return test_text   

def yaml_brackets_cleaner(test_text):
    
    entities = re.finditer("(\[(action|location|date|effect|agent|target)\])",test_text)
    brackets = re.finditer("(\[.*?\])",test_text)
    
    entities = [entity[0] for entity in entities]
    brackets = [bracket[0] for bracket in brackets]
    
    for bracket in brackets:
        if bracket not in entities:
            no_bracket = re.sub("[\[\]]","",bracket)
            test_text = test_text.replace(bracket,no_bracket)

    return test_text  

def yaml_inverter(test_text):
    
    values = re.finditer("(\(.*?\))",test_text)
    entities = re.finditer("(\[.*?\])",test_text)

    yaml_pair = zip(values, entities)

    for value, entity in yaml_pair:
        test_text = test_text.replace(value[0]+entity[0],entity[0]+value[0])
    return test_text

def tweet_cleaner(tweet, annotated=False, lemmatization=False, unidecoder=True):
    
    if type(tweet)==float:
        return tweet
    
    #if tweet == "":
        #return None
    
    if annotated==True:
        tweet = yaml_parentheses_cleaner(tweet)
        tweet = yaml_brackets_cleaner(tweet)
        tweet = yaml_inverter(tweet)
    
    else:
        tweet = yaml_parentheses_cleaner(tweet)
        tweet = yaml_brackets_cleaner(tweet)
    
    #remove hyperlinks
    tweet = re.sub(r'https?:\/\/.*[\r\n]*',"",tweet)
    #remove punctuation except: . - () []
    tweet = re.sub("([^\s\w\[\]\(\).-])","",tweet)
    #remove diacritics (...é, à, etc...)
    tweet = re.sub("(\r\n|\n|\r)"," ",tweet)
    #remove newlinetweet =
    if unidecoder==True:
        tweet = unidecode(tweet)
    
    if lemmatization==True:
        tokens = tokenize(tweet)
        tokens = [lemmatizer.lemmatize(token, pos="v") for token in tokens]
        tokens = [token for token in tokens if token != ""]
        tweet = detokenizer.detokenize(tokens)
        tweet = re.sub("(?<=\])(\s)(?=\()","",tweet)
    
    return tweet