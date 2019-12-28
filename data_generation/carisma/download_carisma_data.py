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
    """
    Function that downloads magnetometer data from the CARISMA network
    To generate the level 1 dataset, we download the 1 Hz data.
      
    The website on which the data is hosted (http://data.carisma.ca/) uses the 
    folder structure : {data_type}/{cadence}/{year}/{month}/{day}/{station_files}
    
    To download data for a given station, for a given year, the script iterates 
    over the month and day numbers (accounting for leap days) and queries for 
    the available files and downloads them.
    
    The files on the website are stored in as a compressed archive. The script
    extracts the files and deletes the downloaded archives.
    
    Inputs
    ----------
    year
        the year for which data is to be downloaded (the CARISMA dataset
        spans from Dec 2013 - present)
        
    station_name
        A list of valid station names are provided in the 'carisma_station_list'
        document

    Output
    -----
    The script downloads the magnetometer data archives and extracts the text
    readable files which have the extension ".F01". The script generates a new 
    folder structure of the format:
    ../level1/{station_name}/{year}/{month}/{station_files}
        
    Example
    --------
    >>> download_carisma_data(2015, "mcmu")
    """
    
    # CARISMA data available here
    carisma_data_url = 'http://data.carisma.ca/FGM/1Hz/'
    
    # This will return '../fdl18-sw1' as the path
    root_folder = Path(os.getcwd()).parent.parent 
    
    # Setting up directory structure
    data_root = os.path.join(root_folder, 'data')
    l1_data = os.path.join(data_root, 'level1')
    station_folder = os.path.join(l1_data, station_name)
    this_year_folder = os.path.join(station_folder, str(year))

    if not os.path.exists(data_root):
        os.mkdir(data_root)
        
    if not os.path.exists(l1_data):
        os.mkdir(l1_data)

    if not os.path.exists(station_folder):
        os.mkdir(station_folder)
     
    if not os.path.exists(this_year_folder):
        os.mkdir(this_year_folder)
    
    months = range(1, 13)
    error_count = 0
    success_count = 0

    for month in months:
        month_str = str(month).zfill(2) # Left pad with zeros

        days = monthrange(year, month)
        days = days[1] # Month range returns first weekday, and ndays(month)stat
        
        data_output_path = os.path.join(this_year_folder, month_str)
        
        if not os.path.exists(data_output_path):
            os.mkdir(data_output_path)
        
        for day in range(1, days + 1):
            day_str = str(day).zfill(2) # Left pad with zeros                
            this_filename = str(year) + month_str + day_str + station_name.upper() + \
                            '.F01.gz'

            fetch_url = carisma_data_url + str(year) + '/' + \
                        month_str + '/' + day_str + '/' + this_filename

            if not os.path.exists(os.path.join(data_output_path, this_filename[:-3])): # If file does not already exist
                check = os.system("wget -q " + fetch_url + " -O " + os.path.join(data_output_path, this_filename))
            
                # Check = 0 for successful download
                if not check:
                    success_count += 1
                    os.system("gunzip -qf " + os.path.join(data_output_path, this_filename))
                else:
                    error_count += 1
                
    print("Attempted downloads: {}, Successful downloads: {}.".format(error_count + success_count, success_count))
