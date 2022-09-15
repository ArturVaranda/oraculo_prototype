# -*- coding: utf-8 -*-
"""
Created on Tue Dec 14 15:46:22 2021

@author: artur_varanda
"""
from dateutil.parser import parse

def arg_list_compare(true_arg_l,pred_arg_l):
    
    if (not pred_arg_l) and (not true_arg_l):
        return (1,1)
    
    if (not pred_arg_l) and (not(not true_arg_l)):
        return (0,0)
    
    if (not(not pred_arg_l)) and (not true_arg_l):
        return (0,0)

    right = 0
    for el in pred_arg_l:
        if el in true_arg_l:
            right += 1
    
    recall = right/len(true_arg_l)
    precision = right/len(pred_arg_l)
    
    return (recall,precision)


def date_compare(true_arg_l,pred_arg_l):
    
    if (not pred_arg_l) and (not true_arg_l):
        return (1,1)
    
    if (not pred_arg_l) and (not(not true_arg_l)):
        return (0,0)
    
    if (not(not pred_arg_l)) and (not true_arg_l):
        return (0,0)
    
    right = 0

    for el in pred_arg_l:
        
        if type(el)==str:
            try:
                el = parse(el)
            except:
                pass
        
        elif type(el)==list:
            if type(el[0])==str:
                el = parse(el[0])
            else:
                el = el[0]
            
        for el_t in true_arg_l:
            
            if type(el_t)==str:
                el_t = parse(el_t)
            try:
                if el.date() == el_t.date():
                    right += 1
            except:
                try:
                    if el[0].date() == el_t.date():
                        right += 1
                except:
                    pass
    
    recall = right/len(true_arg_l)
    precision = right/len(pred_arg_l)
    
    return (recall, precision)

def f1_calc(recall, prec):
    
    sum_scores = recall + prec
    if sum_scores == 0:
        return 0
    else:
        f1 = (2*recall*prec)/sum_scores
        return f1
    
