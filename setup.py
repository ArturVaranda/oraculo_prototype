# -*- coding: utf-8 -*-
"""
Created on Sun Apr 23 11:49:26 2023

@author: artur
"""
from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'ORACULO_prototype'
LONG_DESCRIPTION = 'Prototype for the event extraction and analysis system'

# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="oraculo_prototype", 
        version=VERSION,
        author="Artur Varanda",
        author_email="<arturjorge.varanda@gmail.com>",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[], # add any additional packages that 
        # needs to be installed along with your package. Eg: 'caer'
        
        keywords=['python', 'first package'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Education",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)