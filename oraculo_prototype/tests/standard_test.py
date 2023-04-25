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
from oraculo_prototype.scraper import news_scraper

import pandas as pd
import numpy as np
import time
import os
import re
import shutil
import geopandas
from shapely.geometry import Point

from datetime import datetime, timedelta

today = datetime.today()

def xval_test():
    
    training_excel = os.path.join(os.path.dirname(os.getcwd()), "event_extractor", "tweet_dataset_train_2.xlsx")
    base_df = pd.read_excel(training_excel, sheet_name='tweets')
    output_df = snips_nlu_tester.snips_xval_tester(base_df, use_new_engine=True, to_xlsx=True)
    
    return output_df

    
#2022 VIII 18 - Chromedriver_autoinstaller does not update correctly. Manually switched path to '(...)/scraper/chromedriver.exe'. 
#Test later IOT check if possible to return to automatic updates.
 
def live_twitter_1account_test(test_id, account="@CorbeauNews", 
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
    
    input_df.to_excel("scrap_{}_{}_output.xlsx".format(test_id, account), sheet_name="raw", )
    
    output_df = snips_nlu_event_extractor.snips_nlu_event_extractor(training_df, input_df, use_new_engine=False,
                                                                    threshold=0.987)
    
    output_df.to_excel("test_{}_{}_snips_output.xlsx".format(test_id, account), sheet_name="score", )
        
    output_df = event_geocoder.event_geocoder(output_df)
    output_df = event_merger.event_aggregator(output_df)
    
    output_df.to_excel("test_{}_{}_output.xlsx".format(test_id, account), sheet_name="score", )
    
    return output_df

def live_news_1source_test(test_id, 
                           test_size = 5,
                           source_url_list=["https://corbeaunews-centrafrique.org/"]
                          ):
    

    training_excel = os.path.join(os.path.dirname(os.getcwd()), "event_extractor", "tweet_dataset_train_2.xlsx")
    training_df = pd.read_excel(training_excel, sheet_name='tweets')
    
    input_df = news_scraper.news_pipeline(source_url_list,
                                          cache_articles=False,
                                          store_output=True,
                                          lead_similarity_param=80,
                                          extraction_limit=True,
                                          article_limit=test_size
                                          )
    
    input_df2 = input_df.head(test_size)
    
    output_df = snips_nlu_event_extractor.snips_nlu_event_extractor(training_df, input_df2, use_new_engine=False,
                                                                    threshold=0.987)
    
    output_df.to_excel("test_{}_snips_output.xlsx".format(test_id), sheet_name="score", )
    print("Starting geocoding at {}".format(time.strftime("%H:%M:%S")))
    output_df = event_geocoder.event_geocoder(output_df)
    print("Starting event merger at {}".format(time.strftime("%H:%M:%S")))
    output_df = event_merger.event_aggregator(output_df)
    
    output_df.to_excel("test_{}_output.xlsx".format(test_id), sheet_name="score")
    
    print("Building geographic layer at {}".format(time.strftime("%H:%M:%S")))
    try:
        output_df = output_df.convert_dtypes()
        output_df = output_df.applymap(lambda value : re.sub(r"(\[|\]|\')","",str(value)))
        output_df[["Lat","Long"]] = output_df[["Lat","Long"]].apply(lambda value : pd.to_numeric(value))

        #geometry = [Point(xy) for xy in zip(output_df2.Long, output_df2.Lat)]
        #output_df2 = output_df2.drop(["Lat","Long"], axis=1)
        #output_gdf = geopandas.GeoDataFrame(output_df2, crs="EPSG:4326", geometry=geometry)
        
        output_gdf = geopandas.GeoDataFrame(output_df, crs="EPSG:4326", geometry=geopandas.points_from_xy(output_df.Long, output_df.Lat))
        output_gdf.to_file("test_{}_output_geo.shp".format(test_id))
        output_gdf.to_file("test_{}_output_geo.geojson".format(test_id), driver="GeoJSON")
        
    except:
       print("An error occurred while generating shapefile")
    
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
            