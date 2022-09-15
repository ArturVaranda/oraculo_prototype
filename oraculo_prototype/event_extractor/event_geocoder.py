# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 10:32:29 2022

@author: artur
"""
#input: multiple event db with loc columns output: merged event db
import pandas as pd
import time

from oraculo_prototype.event_extractor import geocoder_utils

def event_geocoder(extracted_event_df):
    
    extracted_event_df = extracted_event_df.loc[extracted_event_df['y_pred']==1]
    
    unq_loc_data = []
    
    for i in extracted_event_df.index:
        
        unq_loc_list = []
        
        loc_info = geocoder_utils.loc_selector(extracted_event_df["pred_location"].loc[i])
        
        time.sleep(15)
        loc_cols =["Name","Adm1","Country","Lat","Long","Type"]
        #print(loc_info)
        
        if loc_info.empty:
            unq_loc_data.append(unq_loc_list)
            continue
        
        elif loc_info is not None:
            for col in loc_cols:
                print(loc_info[col].iloc[0])
                unq_loc_list.append(loc_info[col].iloc[0])
            unq_loc_data.append(unq_loc_list)
        
        else:
            unq_loc_data.append(unq_loc_list)
            continue
        
    unq_loc_df = pd.DataFrame(unq_loc_data, columns=["Loc_name","Adm1","Country","Lat","Long","Type"], 
                          index=extracted_event_df.index.tolist())

    geocoded_event_df = extracted_event_df.join(unq_loc_df)
    
    return geocoded_event_df

#Test ASAP; untested
def geocoder_tester (annotated_extracted_df):
    
    annotated_extracted_df = annotated_extracted_df.loc[annotated_extracted_df['y_pred']==1]
    annotated_extracted_df["geocoding_eval"] = (lambda score : 1 if \
        annotated_extracted_df["Loc_name"] == annotated_extracted_df["Location"] else 0)
        
    score = annotated_extracted_df["geocoding_eval"].mean()
    
    return score
    
        
        
        