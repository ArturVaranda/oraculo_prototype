#also integrates output of different sources
import pandas as pd 

from oraculo_prototype.event_extractor import merger_utils

#def source_integrator(input geocoded_df from various sources, output integrated_event_df)

#Note: must create canonic form for events, enforcing the following features: source (username), 
#format (twitter, site, ...), URL, id

def event_aggregator(integrated_event_df, date_window=1):
    
    #remove once integrator works
    integrated_event_df = merger_utils.event_merger(integrated_event_df)
    
    agg_event_df = integrated_event_df.groupby(by=['event_code'])\
        ["y_pred",
         "pred_relev_prob",
         "en_content",
         "pred_action",
         "pred_agent",
         "pred_target",
         "pred_date",
         "pred_location",
         "Loc_name",
         "Adm1",
         "Country",
         "Lat",
         "Long",
         "Type",
         "event_code",
         #"source",
         #"format",
         #"id",
         #"URL"
         ].agg(lambda text : list(text))
    
    agg_event_df = merger_utils.loc_ev_selector(agg_event_df)
    agg_event_df["pred_date"] = agg_event_df["pred_date"].apply(merger_utils.date_ev_selector)
    
    agg_event_df.sort_values(by="pred_date")
    
    return agg_event_df