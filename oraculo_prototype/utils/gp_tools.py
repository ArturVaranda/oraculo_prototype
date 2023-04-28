# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 15:15:34 2023

@author: artur
"""
import re
import threading
import multiprocessing

from deep_translator import GoogleTranslator

def raw_translator(text, remove_symbols=True):
    
    content = re.sub("(\n)"," ", text)
    
    if remove_symbols == True:
        content = re.sub(r"([\"#@])","",text)
    
    try:
        content = GoogleTranslator(source='auto', target='en').translate(text=text)
        
    except:
        content = text
            
    return content

def thread_viewer():
    multiprocessing.current_process()
    return threading.enumerate()