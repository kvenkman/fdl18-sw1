# -*- coding: utf-8 -*-
'''
Created on Sun Nov 10 22:56:48 2018

@author: kvenkman

Download CARISMA data

Requires Python 3.4+, Unix based system for wget

'''
from pathlib import Path
import os
from calendar import monthrange

def download_carisma_data(year, station_name):

    # CARISMA data available here
    carisma_data_url = 'http://data.carisma.ca/FGM/1Hz/'
    
    # This will return '../fdl18-sw1' as the path
    root_folder = Path(os.getcwd()).parent.parent 
    
    # Setting up directory structure
    data_root = os.path.join(root_folder, 'data')
    l1_data = os.path.join(data_root, 'level1')
    this_year_folder = os.path.join(l1_data, str(year))

    if not os.path.exists(data_root):
        os.mkdir(data_root)
        
    if not os.path.exists(l1_data):
        os.mkdir(l1_data)
    
    if not os.path.exists(this_year_folder):
        os.mkdir(this_year_folder)
    
    months = range(1, 13)

    for month in months:
        month_str = str(month).zfill(2) # Left pad with zeros

        days = monthrange(year, month)
        days = days[1] # Month range returns first weekday, and ndays(month)stat
        
        data_output_path = os.path.join(l1_data, str(year), month_str)
        
        if not os.path.exists(data_output_path):
            os.mkdir(data_output_path)
        
        for day in range(1, days + 1):
            day_str = str(day).zfill(2) # Left pad with zeros                
            this_filename = str(year) + month_str + day_str + station_name.upper() + \
                            '.F01.gz'

            fetch_url = carisma_data_url + str(year) + '/' + \
                        month_str + '/' + day_str + '/' + this_filename

            check = os.system("wget -q " + fetch_url + " " + os.path.join(data_output_path, this_filename))
            
            # Check = 0 for successful download
            if not check:
                success_count += 1
                os.system("gunzip -qf " + this_filename)
            else:
                error_count += 1
                
    print("Attempted downloads: {}, Successful downloads: {}.".format(error_count + success_count, success_count))
