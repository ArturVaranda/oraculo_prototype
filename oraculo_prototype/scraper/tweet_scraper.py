# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 14:38:06 2021

@author: artur
"""
import time
import os

#from textblob import TextBlob
from Scweet.scweet import scrape

test_account_list = ["@RFIAAfrique","@CorbeauNews","@AFPAfrica","RadioNdekeLuka","@RJDH_RCA","@AP_Africa"]

def tweet_scraper_wrapper(account, start_date=None, end_date=time.strftime("%Y-%m-%d"), resume_scraping=True):  
    
    try:    
        scraped_df = scrape(since=start_date, 
                          until=end_date, 
                          from_account = account, 
                          interval=1, 
                          headless=True, 
                          save_images=False, 
                          resume=resume_scraping, 
                          filter_replies=True
                          )
    except FileNotFoundError:
        scraped_df = scrape(since=start_date, 
                          until=end_date, 
                          from_account = account, 
                          interval=1, 
                          headless=True, 
                          save_images=False, 
                          resume=False, 
                          filter_replies=True
                          )
        
    raw_df = scraped_df[['UserName','Timestamp','Embedded_text','Tweet URL']]
    
    raw_df['en_content'] = raw_df['Embedded_text'].apply(lambda text : raw_translator(text, True))
    
    os.system(os.path.join(os.getcwd(),'chromedriver_killer.bat'))
    
    return raw_df



        

