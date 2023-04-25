# -*- coding: utf-8 -*-
"""
Created on Sun Apr 23 23:27:27 2023

@author: artur
"""
import pandas as pd
import re
#import os
import newspaper
import time

from datetime import datetime
from newspaper import news_pool
from oraculo_prototype.utils import gp_tools
from thefuzz import fuzz

today = datetime.today()
test_url_list = ["https://corbeaunews-centrafrique.org/"]

def lead_finder(raw_text="",title="",similarity_param=80):
    
    paragraphs = re.split(r"\n\n", raw_text)
    
    lead_title_similarity = fuzz.partial_ratio(paragraphs[0],title)
    
    if lead_title_similarity >= similarity_param:
        lead = paragraphs[1]
        print("=== lead_finder: First paragraph same as title. \
              Extracting second paragraph as lead instead")
    else:
        lead = paragraphs[0]
    
    translated_lead = gp_tools.raw_translator(lead)
    
    return translated_lead

def news_pipeline(source_url_list, 
                  cache_articles=True, 
                  threads_per_source_param=2, 
                  store_output=False,
                  lead_similarity_param=80,
                  extraction_limit=False,
                  article_limit=10
                  ):
    
    news_df_data = []
    streams_list = []
    
    print("Building sources pool at {}...".format(time.strftime("%H:%M:%S")))
    for source_url in source_url_list:
        
        source_stream = newspaper.build(source_url, memoize_articles=cache_articles)
        streams_list.append(source_stream)
           
    news_pool.set(streams_list, threads_per_source=threads_per_source_param)
    news_pool.join()
    print("...done at {}".format(time.strftime("%H:%M:%S")))
    
    for source_stream in streams_list:
        
        print("Extracting from {} at {}".format(source_stream, time.strftime("%H:%M:%S")))
        print("Found {} articles".format(len(source_stream.articles)))
        i = 1
        for article in source_stream.articles:
            print("Parsing article {} at {}".format(str(i), time.strftime("%H:%M:%S")))          
            
            article.download()
            article.parse()
            print(article.title)
            article_data = [article.source_url,
                            datetime.timestamp(article.publish_date),
                            article.url,
                            article.text,
                            lead_finder(article.text, 
                                        article.title, 
                                        similarity_param=lead_similarity_param)
                            ]
            
            news_df_data.append(article_data)
            if extraction_limit == True and i == article_limit:
                break
            i += 1
    
    news_df = pd.DataFrame(news_df_data, 
                           columns=["source","Timestamp","url","raw_text","en_content"])
    
    #store news_df as excel output
    if store_output == True:
        news_df.to_excel("news_scrap_output_{}.xlsx".format(today.strftime("%Y-%m-%d")), sheet_name="raw", )
    
    return news_df
            
        
            
    