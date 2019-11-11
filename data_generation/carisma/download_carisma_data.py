# -*- coding: utf-8 -*-
"""
Created on Sun Nov 10 22:56:48 2018

@author: kvenkman

Download CARISMA data

Requires Python 3.4+
"""

from pathlib import Path
import os
import numpy as np
import threading
import wget
from calendar import monthrange

pwd = os.getcwd()

def create_carisma_dirstruct():
    # This will return '../fdl18-sw1' as the path
    root_folder = Path(os.getcwd()).parent.parent 
    
    
    years = 2015 + np.arange(3)
    months = 1 + np.arange(12)
    stations = ['talo', 'sach', 'fsim', 'fsmh',
                'rabb', 'mcmu', 'mstk', 'rank',
                'eskj', 'echu', 'gill']

    for year in years:
        try:
            os.mkdir(os.path.join(base_path,str(year)))
        except:
            print("Error")
        for month in months:
            month_str = str(month)
            month_str = '0'+ month_str if (len(month_str) < 2) else month_str

            try:
                os.mkdir(os.path.join(base_path+str(year), month_str))
                print(year, month)
            except:
                print("Error: Folder exists")         

# Don't have to call if dir structure exists
# create_carisma_dirstruct()   

years = 2015 + np.arange(3)
months = 1 + np.arange(12)

#years = [2017] # 2015 + np.arange(3)
#months = 8 + np.arange(5) # 1 + np.arange(12)

stations = ['talo', 'sach', 'fsim', 'fsmh',
            'rabb', 'mcmu', 'mstk', 'rank',
            'eskj', 'echu', 'gill']

base_url = 'http://data.carisma.ca/FGM/1Hz/'

if(os.name == 'nt'):
    base_path = "C:\\Users\\karth\\Desktop\\carisma\\level1\\"
else:
    base_path = "/data/NASAFDL2018/SpaceWeather/Team2-Ryan/carisma/level1/"

for year in years:
    for month in months:
        month_str = str(month)
        month_str = '0'+ month_str if (len(month_str) < 2) else month_str

        days = monthrange(year, month)
        days = days[1]
        
        os.chdir(os.path.join(base_path+str(year), month_str))
        
        for day in np.arange(1, days + 1):
            for station in stations:
                day_str = str(day)
                day_str = '0'+ day_str if (len(day_str) < 2) else day_str
    
                fetch_url=base_url+str(year)+'/'+ \
                    month_str+'/'+day_str+'/'+ \
                    str(year)+month_str+day_str+ \
                    station.upper()+'.F01.gz'
                
                try:
                    file = wget.download(fetch_url)
                    print("Success!: ", fetch_url)
                    
                except:
                    print("Error: ",fetch_url)
 

