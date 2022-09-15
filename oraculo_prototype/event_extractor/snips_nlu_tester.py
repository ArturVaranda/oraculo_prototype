# -*- coding: utf-8 -*-
"""
Created on Tue Dec 14 15:10:12 2021

@author: artur_varanda
"""
import pandas as pd
import time

from oraculo_prototype.event_extractor import snips_nlu_event_extractor
from oraculo_prototype.event_extractor import score_utils

from sklearn import model_selection
from sklearn import metrics
from statistics import mean

#to-do: convert to object-based code, i.e., tweet.agent.recall
def score_computer(test_df):
    
    y_true = pd.Series(test_df['Relevant?'])
    y_pred = pd.Series(test_df['y_pred'])
                                       
    score_df_data = []
    
    f1 = metrics.f1_score(y_true,y_pred)
    score_df_data.append(f1)
    kappa = metrics.cohen_kappa_score(y_true,y_pred)
    score_df_data.append(kappa)
    auroc = metrics.roc_auc_score(y_true,y_pred)
    score_df_data.append(auroc)
    accuracy = metrics.accuracy_score(y_true,y_pred)
    score_df_data.append(accuracy)
    balanced_acc = metrics.balanced_accuracy_score(y_true,y_pred)
    score_df_data.append(balanced_acc)
    precision = metrics.precision_score(y_true,y_pred)
    score_df_data.append(precision)
    recall = metrics.recall_score(y_true,y_pred)
    score_df_data.append(recall)
    
     
    tweet_score_df_data = []
    
    for j in range(len(test_df)):
        
        tweet_score = []
        
        if test_df["Relevant?"].iloc[j]==1:

            if test_df.y_pred.iloc[j]==1:
                
                action = test_df['Action'].iloc[j]
                pred_action = test_df['pred_action'].iloc[j]
                action_recall = score_utils.arg_list_compare(action,pred_action)[0]
                action_precision = score_utils.arg_list_compare(action,pred_action)[1]
                action_f1 = score_utils.f1_calc(action_recall,action_precision)
                tweet_score.append(action_recall)
                tweet_score.append(action_precision)
                tweet_score.append(action_f1)


                agent = test_df['Agent'].iloc[j]
                pred_agent = test_df['pred_agent'].iloc[j]
                agent_recall = score_utils.arg_list_compare(agent,pred_agent)[0]
                agent_precision = score_utils.arg_list_compare(agent,pred_agent)[1]
                agent_f1 = score_utils.f1_calc(agent_recall,agent_precision)
                tweet_score.append(agent_recall)
                tweet_score.append(agent_precision)
                tweet_score.append(agent_f1)


                target = test_df['Target'].iloc[j]
                pred_target = test_df['pred_target'].iloc[j]
                target_recall = score_utils.arg_list_compare(target,pred_target)[0]
                target_precision = score_utils.arg_list_compare(target,pred_target)[1]
                target_f1 = score_utils.f1_calc(target_recall,target_precision)
                tweet_score.append(target_recall)
                tweet_score.append(target_precision)
                tweet_score.append(target_f1)


                date = test_df['Date2'].iloc[j]
                pred_date = test_df['pred_date'].iloc[j]
                date_recall = score_utils.date_compare(date,pred_date)[0]
                date_precision = score_utils.date_compare(date,pred_date)[1]
                date_f1 = score_utils.f1_calc(date_recall,date_precision)
                tweet_score.append(date_recall)
                tweet_score.append(date_precision)
                tweet_score.append(date_f1)


                location = test_df['Location'].iloc[j]
                pred_location = test_df['pred_location'].iloc[j]
                location_recall = score_utils.arg_list_compare(location,pred_location)[0]
                location_precision = score_utils.arg_list_compare(location,pred_location)[1]
                location_f1 = score_utils.f1_calc(location_recall,location_precision)
                tweet_score.append(location_recall)
                tweet_score.append(location_precision)
                tweet_score.append(location_f1)


                effects = test_df['Effects'].iloc[j]
                pred_effects = test_df['pred_effects'].iloc[j]
                effects_recall = score_utils.arg_list_compare(effects,pred_effects)[0]
                effects_precision = score_utils.arg_list_compare(effects,pred_effects)[1]
                effects_f1 = score_utils.f1_calc(effects_recall,effects_precision)
                tweet_score.append(effects_recall)
                tweet_score.append(effects_precision)
                tweet_score.append(effects_f1)


                avg_tweet_recall = mean([action_recall,agent_recall,target_recall,date_recall,location_recall,effects_recall])
                tweet_score.append(avg_tweet_recall)
                avg_tweet_precision = mean([action_precision,agent_precision,target_precision,date_precision,location_precision,effects_precision])
                tweet_score.append(avg_tweet_precision)
                avg_tweet_f1 = mean([action_f1,agent_f1,target_f1,date_f1,location_f1,effects_f1])
                tweet_score.append(avg_tweet_f1)
                
                tweet_score_df_data.append(tweet_score)
                
    tweet_score_df = pd.DataFrame(data=tweet_score_df_data, columns = ['action recall','action prec','action f1',
                                                                       'agent recall','agent prec','agent f1',
                                                                       'target recall','target prec','target f1',
                                                                       'date recall','date prec','date f1',
                                                                       'location recall','loc prec','loc f1',
                                                                       'effects recall','effects prec','effects f1',
                                                                       'overall recall','overall prec','overall f1'])
    
    #compute mean of columns, export to list

    for column in tweet_score_df.columns:
        score_df_data.append(tweet_score_df[column].mean())
    
    score_df = pd.DataFrame(data=[score_df_data], columns = ['f1',
                                                           'kappa',
                                                           'auroc',
                                                           'accuracy',
                                                           'balanced_accuracy',
                                                           'precision',
                                                           'relevance recall',
                                                           'action recall','action prec','action f1',
                                                           'agent recall','agent prec','agent f1',
                                                           'target recall','target prec','target f1',
                                                           'date recall','date prec','date f1',
                                                           'location recall','loc prec','loc f1',
                                                           'effects recall','effects prec','effects f1',
                                                           'overall recall','overall prec','overall f1'
                                                           ])
    
    return score_df

def snips_xval_tester(base_df, test_id="001", nr_splits=5, threshold=0.85,
                      oversampling=False, ratio=1, use_new_engine=False, to_xlsx=True):
    
    ov_xval_score_df = pd.DataFrame(columns = ['test_id',
                                               'train_size',
                                               'test_size',
                                               'nr_splits',
                                               'oversampling',
                                               'new_engine',
                                               'threshold',
                                               'runnning_time',
                                               'f1',
                                               'kappa',
                                               'auroc',
                                               'accuracy',
                                               'balanced_accuracy',
                                               'precision',
                                               'relevance recall',
                                               'action recall','action prec','action f1',
                                               'agent recall','agent prec','agent f1',
                                               'target recall','target prec','target f1',
                                               'date recall','date prec','date f1',
                                               'location recall','loc prec','loc f1',
                                               'effects recall','effects prec','effects f1',
                                               'overall recall','overall prec','overall f1']
                                    )
                                               
    
    print("Starting at {}".format(str(time.strftime("%H:%M:%S")))) 
    
    base_df = snips_nlu_event_extractor.base_df_cleaner(base_df)
    split_nr = 1
    skf = model_selection.StratifiedKFold(n_splits=nr_splits, shuffle=True, random_state=39)
    
    y = pd.Series(base_df['Relevant?'])
    X = pd.Series(base_df['Annotated Tweet'])
    
    for train_index, test_index in skf.split(X, y):
        
        start_time_split = time.time()
        print("\nStarting split {0} at {1}".format(str(split_nr),str(time.strftime("%H:%M:%S", time.localtime()))))
        
        train_index = train_index.tolist()
        test_index = test_index.tolist()
        
        train_df = base_df.iloc[train_index]
        test_df = base_df.iloc[test_index]
        
        print("Generating training file...")
        try: 
            training_dataset = snips_nlu_event_extractor.training_file_creator(train_df, oversampling=oversampling, 
                                                        ratio= ratio)
            print("...done")
        except:
            print("Could not create training file. Exiting.")
            return None
        
        test_df = snips_nlu_event_extractor.snips_nlu_event_extractor(training_dataset, 
                                                                      test_df, 
                                                                      use_new_engine=use_new_engine, 
                                                                      threshold=threshold)
        if to_xlsx == True:
            test_df.to_excel('snips_nlu_outputs\\test_{}_split_{}_output.xlsx'.format(test_id, split_nr))
        
        print("\nStarting evaluation of split {0} at {1}".format(str(split_nr),str(time.strftime("%H:%M:%S", time.localtime()))))
        
        split_score_df = score_computer(test_df)
        
        end_time_split = time.time()
        
        split_parameters_df = pd.DataFrame(data = [[split_nr, 
                                                   len(train_df), 
                                                   len(test_df), 
                                                   nr_splits, 
                                                   oversampling, 
                                                   use_new_engine, 
                                                   threshold, 
                                                   (end_time_split - start_time_split)]],
                                           columns = ['test_id',
                                                      'train_size',
                                                      'test_size',
                                                      'nr_splits',
                                                      'oversampling',
                                                      'new_engine',
                                                      'threshold',
                                                      'runnning_time']
                                           )
        
        split_full_score_df = pd.concat([split_parameters_df, split_score_df], axis=1)
        
        ov_xval_score_df = pd.concat([ov_xval_score_df,split_full_score_df])
        
        split_nr+=1 
        
    ov_xval_score_df.reset_index()
        
    if to_xlsx == True:
        ov_xval_score_df.to_excel('snips_nlu_outputs\\snips_args_xval_test_output_{}.xlsx'.format(test_id), sheet_name = 'metrics')
    
    return ov_xval_score_df
        
        
        
        
        
        
        
        
        
        

