#This script downloads GNSS ISMR (Ionospheric scintillation monitoring receiver) data from 
#the Canadian High Arctic Ionospheric Network (chain.physics.unb.ca)
#and extracts raw and derived GNSS data and writes it into a .csv. 

#Note: The data is just being downloaded and written into a .csv and is not being preprocessed.

#*** How to run the script ****

#Define 'datetime_start' and 'input_datetimes' in the main fucntion and run the 
#script as: "python Download_GNSS_CHAIN_data.py number_of_days". 
#Fill number_of_days with the total number of days you would like to download data.

#Please create your own account in chain.physics.unb.ca with your own email before running this script.




#let's get some libraries imported :)
import numpy as np
import pandas as pd
import sys
import datetime
import ftplib
import sys
import calendar
import os.path
import time
import multiprocessing
import datetime
from urllib.request import urlopen
from urllib.request import urlretrieve 
from urllib.request import urlcleanup

# spacepy imports
#	sys.path.append('/Users/ryanmc/Documents/spacepy-0.1.6/')
#sys.path.append('/usr/local/sdeploy/spacepy/')
sys.path.append('/home/kibrom/spacepy-0.2.1/')


import socket
socket.setdefaulttimeout(180)



def PolaRxS_batchDataDownloadToLocal(input_datetime):


	# start timer
	start_timer = time.time()

	#local path where the script and spreadsheet exists
	local_pwd = '/home/kibrom/kwork/sw-GNSS/fdl18_Frontiers/GNSS_data_derived_products'
	#sub directory to put raw data
	level1 = '/level1/'
	#CHAIN data labels spreadsheet
	data_labels = '/CHAIN_data_labels.xlsx'
	#how long is the downloading time
	download_time = 'data_download_runtimes/'

	#Python makes directories if they don't exist

	if not os.path.exists(local_pwd + level1):
		os.makedirs(local_pwd + level1)

	if not os.path.exists(local_pwd + level1 + download_time ):
		os.makedirs(local_pwd + level1 + download_time)


	df_labels_PolaRxS = pd.read_excel(local_pwd + data_labels, sheet_name='PolaRxS_labels', header=None, usecols=[1])

	# input python function to generate daily data here
	print('-------> working on datetime = {0}'.format(input_datetime))
	
	thisdatetime = input_datetime
	thisdoy = thisdatetime.timetuple().tm_yday               
	thisyr = thisdatetime.year
	thisdy = thisdatetime.day
	thismon = thisdatetime.month

	# Initialize the daily dataframe
	df_save = pd.DataFrame()
	#      SET DESIRED DIRECTORY HERE
	save_data = local_pwd + level1
        # File name to save the the full day of data to local disk
	filename_save = save_data + 'PolaRxS_CHAINdata__' + format(thisyr,'04') + '_' + format(thisdoy,'03') + '.csv'
	if os.path.isfile(filename_save):
		print('The data for this doy is available at: ' )
		print(filename_save)
		sys.exit('We have data for this doy')

	for h in range(0,1): 
	#for h in range(0,24):
	
		print('this date = {0}'.format(thisdatetime))

		hour_dir = '/gps/ismr/' + '{:04}'.format(int(thisyr)) + '/' + '{:03}'.format(int(thisdoy)) + '/' + '{:02}'.format(int(thisdatetime.hour+h)) + '/'

		print('this hour directory = {0}'.format(hour_dir))
	
		#Get files for current hour
		try: 
			ftp = ftplib.FTP("chain.physics.unb.ca")
			ftp.login("kibebuy@gmail.com","4Kindahafti4")
			ftp.cwd(hour_dir)
			#List the files in the current directory
			files_thishour = ftp.nlst()
		except Exception as e: 
			print('\n-------unable to login, change to directory, or list files {0}'.format(hour_dir))
			print('with error {0}--------\n'.format(e))
			continue


		for s in range(len(files_thishour)):
#			print('this station file = {0}'.format(files_thishour[s]))	
			
			# establish and make, if necessary, a local directory for the data
			local_dir = save_data
#			local_fn_and_dir = local_dir + files_thishour[s]
			local_fn_and_dir = local_dir + files_thishour[s][-18:]
# 			print('local_fn_and_dir = {0}'.format(local_fn_and_dir))

#			if not os.path.exists(local_dir):
#				os.makedirs(local_dir)

			# clean up the cache that may have been created by previous calls to urlretrieve
			urlcleanup()

			# download the data for the current hour
			if not os.path.isfile(local_fn_and_dir):
				urlretrieve('ftp://kibebuy@gmail.com:4Kindahafti4@chain.physics.unb.ca/'+hour_dir[1:]+files_thishour[s],local_fn_and_dir)
				
			try:
				txt_thishour_thisfile = np.genfromtxt(local_fn_and_dir, delimiter=",", filling_values=99)
	#             df_thishour_thisfile = pd.DataFrame(np.genfromtxt(local_fn_and_dir, delimiter=",", filling_values=99),columns=df_labels_PolaRxS[1].tolist())

				#print np.shape(txt_thishour_thisfile)
			except:
				print('\n\n ***unable to read {} ***\n\n'.format(local_fn_and_dir))
				continue
		
			thisabbr = local_fn_and_dir[-18:-15]

			# Remove KUG station due to bias
			if thisabbr == 'kug':
# 				print('\n\n skipping kugc... \n\n')
				os.remove(local_fn_and_dir)
				continue
			
			if len(txt_thishour_thisfile) == 0:
				print('\n\n ***file is empty, continuing***\n\n')
				os.remove(local_fn_and_dir)
				continue
			
			os.remove(local_fn_and_dir)

			df_thishour_thisfile = pd.DataFrame(data=txt_thishour_thisfile,columns=df_labels_PolaRxS[0].tolist())
			df_thishour_thisfile['CHAIN station'] = pd.Series( np.full( (len(txt_thishour_thisfile[:,0])),thisabbr ) )
			
			# Concatenate the new dataframe to the existing dataframe
			df_save = pd.concat( [df_save,df_thishour_thisfile] )
			
			del df_thishour_thisfile	
					
			
	# Save the full day of data to local disk

	pd.DataFrame.to_csv(df_save,filename_save,na_rep='NaN')
	del filename_save 

	
	# end timer
	end_timer = time.time()
	runtime_thisday = end_timer - start_timer
	np.savetxt(save_data + download_time + 'runtime__' + format(thisyr,'04') + '_' + format(thisdoy,'03') + '.txt',np.array(runtime_thisday).reshape(1,),fmt='%.2f')

def main():

	# note: sys.argv[0] is the script name
	numprocessors = int(sys.argv[1])
	print('numprocessors = {0}'.format(numprocessors))
	
	pool = multiprocessing.Pool(numprocessors)
	datetime_start = datetime.datetime(2014,1,1)
	input_datetimes = [ (datetime_start + datetime.timedelta(days=d)) for d in (range(2)) ]
	pool.map(PolaRxS_batchDataDownloadToLocal,input_datetimes) 


if __name__=='__main__':
	main()
