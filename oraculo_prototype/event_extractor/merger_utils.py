# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 14:41:08 2021

@author: artur
"""
import pandas as pd

def event_merger(geocoded_event_df, date_window=2, event_id_start=0):
    
    # 0) start id / convert to datetime
    event_id = event_id_start
    geocoded_event_df["pred_date"] = geocoded_event_df["pred_date"].apply(lambda date : 
                                                                          pd.to_datetime(date, unit="s").date())
    
    # 1) First pass: create subset of most informative tweets
    geocoded_event_df["event_code"]=""
    geocoded_event_df = geocoded_event_df.sort_values(by='pred_date')
    best_tweets_df = geocoded_event_df.dropna(subset=['Loc_name', 'Adm1'])
    best_tweets_df = best_tweets_df.loc[(geocoded_event_df.Loc_name!=geocoded_event_df.Country)]
    
    # 1.1) iterate tweets sorted by their event pred_date
    for i in best_tweets_df.index:
        
        local_date = best_tweets_df.pred_date.loc[i]
        local_loc = best_tweets_df.Loc_name.loc[i]
        local_adm1 = best_tweets_df.Adm1.loc[i]
        local_cty = best_tweets_df.Country.loc[i]
        
        # 3) test if tweet was already assigned an event code; if so, skip
        if geocoded_event_df.event_code.loc[i] != "":
            continue
        else:
            geocoded_event_df.event_code.loc[i] = str(event_id)
        
        # 4) create subset df of tweets without event codes based on their dates
        subset_df = geocoded_event_df.loc[((geocoded_event_df.pred_date>=local_date-pd.DateOffset(date_window))&
                                     (geocoded_event_df.pred_date<=local_date+pd.DateOffset(date_window)))&
                                    (geocoded_event_df.event_code=="")&(geocoded_event_df.Country==local_cty)]
        
        # 5) assign event codes according to event *Loc_name*
        for j in subset_df.index:
            
            if geocoded_event_df.event_code.loc[j]!="":
                continue
                
            if geocoded_event_df.Loc_name.loc[j]==local_loc or \
                geocoded_event_df.Loc_name.loc[j]==local_adm1 or \
                geocoded_event_df.Loc_name.loc[j]==local_cty:
                    
                geocoded_event_df.event_code.loc[j] = str(event_id)
        
        # 6) assign event codes according to event *Adm1*
            if geocoded_event_df.Adm1.loc[j]==local_adm1:
                geocoded_event_df.event_code.loc[j] = str(event_id)
        
        event_id +=1
    
    #Second pass
    for i in geocoded_event_df.index:
        
        local_date = geocoded_event_df.pred_date.loc[i]
        local_loc = geocoded_event_df.Loc_name.loc[i]
        if type(geocoded_event_df.Adm1.loc[i]) != float:
            local_adm1 = geocoded_event_df.Adm1.loc[i]
        if type(geocoded_event_df.Country.loc[i]) != float:
            local_cty = geocoded_event_df.Country.loc[i]
        
        # 3) test if tweet was already assigned an event code; if so, skip
        if geocoded_event_df.event_code.loc[i] != "":
            continue
        else:
            geocoded_event_df.event_code.loc[i] = str(event_id)
        
        # 4) create subset df of tweets without event codes based on their dates
        subset_df = geocoded_event_df.loc[((geocoded_event_df.pred_date>=local_date)&
                                     (geocoded_event_df.pred_date<=local_date+pd.DateOffset(date_window)))&
                                    (geocoded_event_df.event_code=="")&(geocoded_event_df.Country==local_cty)]
        
        # 5) assign event codes according to event *Loc_name*
        for j in subset_df.index:
            
            if geocoded_event_df.event_code.loc[j]!="":
                continue
                
            if geocoded_event_df.Loc_name.loc[j]==local_loc or \
                geocoded_event_df.Loc_name.loc[j]==local_adm1 or \
                geocoded_event_df.Loc_name.loc[j]==local_cty:
                    
                geocoded_event_df.event_code.loc[j] = str(event_id)
        
        # 6) assign event codes according to event *Adm1*
            if geocoded_event_df.Adm1.loc[j]==local_adm1:
                geocoded_event_df.event_code.loc[j] = str(event_id)
        
        event_id +=1
    
    return geocoded_event_df

def date_ev_selector(date_list):
    
    date_list = sorted(date_list)
    
    if len(date_list)>=1:
        return date_list[0]
    else:
        return None
    
def loc_ev_selector(agg_ev_dataset):
    
    cols = ["Loc_name","Adm1","Country","Lat","Long","Type"]
    
    for i in agg_ev_dataset.index:
        
        loc_list = agg_ev_dataset.Loc_name.loc[i]
        adm1_list = agg_ev_dataset.Adm1.loc[i]
        cty_list = agg_ev_dataset.Country.loc[i]
        lat_list = agg_ev_dataset.Lat.loc[i]
        long_list = agg_ev_dataset.Long.loc[i]
        type_list = agg_ev_dataset.Type.loc[i]
        
        decision_df = pd.DataFrame({"Loc_name" : loc_list,
                                    "Adm1" : adm1_list,
                                    "Country" : cty_list,
                                    "Lat" : lat_list,
                                    "Long" : long_list,
                                    "Type" : type_list})
        
        decision_df = decision_df.sort_values(by="Type", ascending=False)
        
        for col in cols:
            agg_ev_dataset.at[i, col]=decision_df[col].iloc[0]
    
    return agg_ev_dataset

def merger_tester (annotated_extracted_df):

    annotated_extracted_df["eval?"]=""
    ov_acc = []    

    for idx in annotated_extracted_df.index:
        
        if (annotated_extracted_df["eval?"].loc[idx] == 1) or (annotated_extracted_df["eval?"].loc[idx] == 0):
            continue
        
        l_human_code = annotated_extracted_df['Event code'].loc[idx]
        l_mech_code = annotated_extracted_df['event_code'].loc[idx]
        
        subset = annotated_extracted_df.loc[(annotated_extracted_df['Event code']==l_human_code)|(annotated_extracted_df['event_code']==l_mech_code)]
        #print(len(subset))
        
        for idx2 in subset.index:
            
            if (subset['event_code'].loc[idx2]==l_mech_code) and (subset['Event code'].loc[idx2]==l_human_code):
                annotated_extracted_df.at[idx2, "eval?"] = 1
            
            else:
                annotated_extracted_df.at[idx2, "eval?"] = 0
            
    ov_acc = annotated_extracted_df["eval?"].mean()

    return ov_acc