# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 14:40:47 2021

@author: artur_varanda
"""
#inputs: train_df (training dataframe from excel file); raw_df (test dataframe
#  or dataframe to be classified); training_file_template (yaml file)
#parameters: mode = [test or operation], xval = [y/n], folds = [n]
#outputs: tweet_df (tagged raw_df)

#imports
import pandas as pd
import os
import io
import json
import time
import signal

from oraculo_prototype.event_extractor import tweet_utils
from oraculo_prototype.utils import gp_tools

from shutil import copyfile
from sklearn.utils import resample
from snips_nlu import SnipsNLUEngine
from snips_nlu.default_configs import CONFIG_EN

dir_path = os.path.dirname(os.path.realpath(__file__))

#template training file, blank training file and training xlsx file
template = os.path.join(dir_path,'snips_training_file_example.yaml')
training_file = os.path.join(dir_path,'snips_training_file.yaml')
entities_file = os.path.join(dir_path,"snips_training_file_entities.yaml")


def new_engine(training_dataset):

    nlu_engine = SnipsNLUEngine(config=CONFIG_EN)
    nlu_engine.fit(training_dataset, force_retrain=True)
    try:
        nlu_engine.persist(os.path.join(dir_path,"engine"))
    except:
        pass
    
    return nlu_engine

def base_df_cleaner(base_df):
    
    arg_cols = ["Action","Agent","Target","Date2","Location","Effects"]
    for col in arg_cols:
        base_df[col] = base_df[col].apply(tweet_utils.arg_cleaner)
        
    base_df['en_content'] = base_df['en_content'].apply(tweet_utils.tweet_cleaner, args=(False,False,True))
    base_df['Annotated Tweet'] = base_df['Annotated Tweet'].apply(tweet_utils.tweet_cleaner, \
                                                                  args=(True,False,True))

    return base_df

    
#creates json, yaml files
def training_file_creator(train_df, oversampling=False, ratio=1):
    
    X = pd.Series(train_df['Annotated Tweet'])
    
    training = X.dropna()
    
    if oversampling == True:
        training = resample(training, replace = True, n_samples = int(len(training)/ratio))

    copyfile(template, training_file)
        
    #create training file from yaml
    with open(training_file, 'a', encoding="utf-8") as training_f:
        for tweet in training:
            
            training_f.write("\n  - "+'"'+str(tweet)+'"')
    
    os.system("cd {}".format(dir_path))
    os.system("activate oraculo-env-py3")
    os.system("snips-nlu generate-dataset en {} \
            snips_training_file.yaml > snips_train.json".format(entities_file))
            
    with io.open(os.path.join(dir_path,"snips_train.json")) as f:
            training_dataset = json.load(f)
    
    return training_dataset
    
    

#start conditions: clean raw_df, index from train_test_splitter
def snips_nlu_event_extractor(training_file, raw_df, use_new_engine = False, \
                              threshold = 0.85, raw_df_content_name="en_content", \
                              raw_df_date_name = "Timestamp"):
    
    with io.open(os.path.join(dir_path,"snips_train.json")) as f:
            training_dataset = json.load(f)
    
    if use_new_engine == False:
        try:
            print("Retrieving trained engine...")
            nlu_engine = SnipsNLUEngine.from_path(os.path.join(dir_path,"engine"))
            print("...done.")
        except:
            print("Failed engine retrieval. Training new engine...")
            nlu_engine = new_engine(training_dataset)
            print("...training done")
    else:
        print("Training new engine...")
        nlu_engine = new_engine(training_dataset)
        print("...training done")
    
    y_pred = []
    args_pred = []
    test_step = 0
    
    #preprocess raw_df
    raw_df[raw_df_content_name] = raw_df[raw_df_content_name].apply(tweet_utils.tweet_cleaner, args=(False,False,True))    

    for i in raw_df.index:
        test_step += 1
        print("Classifying content {} of {} at {}".format(test_step,len(raw_df.index),time.strftime("%H:%M:%S")))
        event_args = {
                       'action':[],
                       'agent':[],
                       'target':[],
                       'date':[],
                       'location':[],
                       'effect':[]
                      }

        tweet = raw_df[raw_df_content_name].loc[i]
        
        signal.signal(signal.SIGALRM, gp_tools.handle_timeout)
        signal.alarm(600)  # 10*60 seconds = 10 minutes

        #TODO: Try for time
        try:
            intents = nlu_engine.get_intents(str(tweet))
            
        except Exception as e1:
            print("Failed intent extraction due to {}".format(e1))
            print(tweet+" at index: "+str(i)+" at {}".format(str(time.strftime("%H:%M:%S", time.localtime()))))
            y_pred.append(0)
            loc_y_pred = 0               
            rel_prob = 0
            for arg in event_args.keys():
                event_args[arg].append("")
            
        else:
            for intent in intents:
                if intent['intentName']=='event':
                    rel_prob = intent['probability']

                    if rel_prob >= threshold:
                        y_pred.append(1)
                        loc_y_pred = 1
                    else:
                        y_pred.append(0)
                        loc_y_pred = 0
            
            signal.signal(signal.SIGALRM, gp_tools.handle_timeout)
            signal.alarm(600)  # 10*60 seconds = 10 minutes
            
            try:
                print("Starting argument extraction at {}".format(time.strftime("%H:%M:%S")))
                slots = nlu_engine.get_slots(str(tweet),'event')
            except Exception as e:
                for arg in event_args.keys():
                    event_args[arg].append("")
                print("Failed argument extraction due to {}".format(e))
            else:
                for slot in slots:
                    for arg in event_args.keys():
                        if slot['slotName']==arg:
                            try:
                                event_args[arg].append(slot['value']['value'])
                            except:
                                event_args[arg].append("")
            
                event_args['date']=raw_df[raw_df_date_name].loc[i] #"Timestamp" is format-dependent!
                print("Argument extraction was successful")
                signal.alarm(0) # reset alarm
                
        #if not event_args['date']:
            #event_args['date'].append(test_df.date.loc[i])

        tweet_args = [loc_y_pred,rel_prob,event_args['action'],event_args['agent'],event_args['target'],
                         event_args['date'],event_args['location'],event_args['effect']]

        args_pred.append(tweet_args)
    
    pred_args_df = pd.DataFrame(args_pred, columns=['y_pred','pred_relev_prob','pred_action','pred_agent','pred_target',
                                                'pred_date','pred_location','pred_effects'],
                           index = raw_df.index)

    extracted_event_df = pd.merge(raw_df, pred_args_df, how='inner',left_index=True, right_index=True)
    
    return extracted_event_df


    