# -*- coding: utf-8 -*-
"""
Created on Tue Jan  4 10:51:05 2022

@author: artur
"""
from oraculo_prototype.event_extractor import snips_nlu_event_extractor
from oraculo_prototype.event_extractor import snips_nlu_tester
from oraculo_prototype.event_extractor import event_geocoder
from oraculo_prototype.event_extractor import event_merger
from oraculo_prototype.scraper import tweet_scraper

import pandas as pd
import os
import shutil

from datetime import datetime, timedelta

today = datetime.today()

def xval_test():
    
    training_excel = os.path.join(os.path.dirname(os.getcwd()), "event_extractor", "tweet_dataset_train_2.xlsx")
    base_df = pd.read_excel(training_excel, sheet_name='tweets')
    output_df = snips_nlu_tester.snips_xval_tester(base_df, use_new_engine=True, to_xlsx=True)
    
    return output_df

    
#2022 VIII 18 - Chromedriver_autoinstaller does not update correctly. Manually switched path to '(...)/scraper/chromedriver.exe'. 
#Test later OIT check if possible to return to automatic updates.
 
def live_1account_test(test_id, account="@CorbeauNews", 
                       purge = True,
                       start_date_delta=3, 
                       end_date=today.strftime("%Y-%m-%d"), 
                       resume_scraping=False):
    
    if purge == True:
        tweet_scraper_outputs_purge()
    
    start_date = (today - timedelta(days = start_date_delta)).strftime("%Y-%m-%d")
    
    training_excel = os.path.join(os.path.dirname(os.getcwd()), "event_extractor", "tweet_dataset_train_2.xlsx")
    training_df = pd.read_excel(training_excel, sheet_name='tweets')
    
    input_df = tweet_scraper.tweet_scraper_wrapper(account=account,
                                                   start_date=start_date,
                                                   end_date=end_date,
                                                   resume_scraping=resume_scraping)
    
    output_df = snips_nlu_event_extractor.snips_nlu_event_extractor(training_df, input_df, use_new_engine=False,
                                                                    threshold=0.987)
    
    output_df.to_excel("test_{}_{}_snips_output.xlsx".format(test_id, account), sheet_name="score", )
        
    output_df = event_geocoder.event_geocoder(output_df)
    output_df = event_merger.event_aggregator(output_df)
    
    output_df.to_excel("test_{}_{}_output.xlsx".format(test_id, account), sheet_name="score", )
    
    return output_df
    
def tweet_scraper_outputs_purge():
    
    folder = os.path.join(os.getcwd(), "outputs")
    
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))
            