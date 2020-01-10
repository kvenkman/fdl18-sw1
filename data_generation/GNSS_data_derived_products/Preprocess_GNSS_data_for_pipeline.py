# THE DEVELOPMENT OF THIS SCRIPT STARTED 4.14.2018, BUT ON HOLD FOR TIME BEING. NEED TO DO THIS
#  PROPERLY THROUGH THE USE OF TEXT FILES AND RDDS. -RMM 4.14.2018

#-----------------------------------------------------------------------------------------
# General imports


import numpy as np
import pandas as pd
import datetime
import sys
import os.path
#from geospacepy import satplottools,special_datetime
import time
import sys
import multiprocessing


#-----------------------------------------------------------------------------------------
# Define needed functions 

def llh2ecef (flati,floni, altkmi ):
#         lat,lon,height to xyz vector
#
# input:
# flat      geodetic latitude in deg
# flon      longitude in deg
# altkm     altitude in km
# output:
# returns vector x 3 long ECEF in km

	flat  = float(flati);
	flon  = float(floni);
	altkm = float(altkmi);
	
	clat = np.cos(np.radians(flat));
	slat = np.sin(np.radians(flat));
	clon = np.cos(np.radians(flon));
	slon = np.sin(np.radians(flon));

	# from Vallado Algorithm 50 (page 426) to produce r_delta and r_k variables
	R_earth = 6378.137
	e_earth = 0.081819221456 # Earth's eccentricity, or flattening
	C_earth = R_earth / np.sqrt(1 - e_earth*e_earth*np.sin(flat)*np.sin(flat))
	S_earth = C_earth*(1-e_earth*e_earth)

	x      =  (C_earth + altkm) * clat * clon;
	y      =  (C_earth + altkm) * clat * slon;
	z      =  ( S_earth + altkm ) * slat;

	return x,y,z
	
# following function obtained from: https://www.reddit.com/r/GISscripts/comments/1borl4/python_convert_gps_week_seconds_to_normal_time/
#def GPS_weekseconds_to_utc(gpsweek,gpsseconds,leapseconds):
#    #import datetime, calendar
#    datetimeformat = "%Y-%m-%d %H:%M:%S"
#    epoch = datetime.datetime.strptime("1980-01-06 00:00:00",datetimeformat)
#    elapsed = datetime.timedelta(days=(gpsweek*7),seconds=(gpsseconds+leapseconds))
#    tmp = datetime.datetime.strftime(epoch + elapsed,datetimeformat)
#    utc = float(tmp[11:13])*3600. + float(tmp[14:16])*60. + float(tmp[17:19])
#    return utc
def GPS_weekseconds_to_utc(gpsweek,gpsseconds,leapseconds):
	# NOTE: this function returns ut seconds into day (NOT ut seconds into current hour)
	#      i.e., 1 hour utc = 3600; 4 hours utc = 3600*4
	#import datetime, calendar
	datetimeformat = "%Y-%m-%d %H:%M:%S"
	epoch = datetime.datetime.strptime("1980-01-06 00:00:00",datetimeformat)
	thesedays = np.array([datetime.timedelta(days=i) for i in (gpsweek*7)])
	#theseseconds = np.array([datetime.timedelta(seconds=i) for i in (gpsseconds+17)])
		# If NOT applying leapseconds adjustment
	theseseconds = np.array([datetime.timedelta(seconds=i) for i in (gpsseconds)])
	tmp = epoch + thesedays + theseseconds
	#days = [tmp[i].day for i in range(0,len(tmp))] # this returns the day of the month
	days = [tmp[i].timetuple().tm_yday for i in range(0,len(tmp))] # this returns the day of the year
	utc = [tmp[i].hour*3600. + tmp[i].minute*60 + tmp[i].second for i in range(0,len(tmp))]
	return days, utc
#-----------------------------------------------------------------------------------------


def PolaRxS_MLDatabaseGeneration(input_datetime):

	print('-------> working on datetime = {0}'.format(input_datetime))
	
	# ----------------------------------------------------------------------------------------
	# ----------------------------------------------------------------------------------------
	# 
	# Script to identify periods of time and compile and preprocess GPS/GNSS scintillation data and label it for a pipeline 
	#     NOTE: This script was duplicated from the 'CHAIN_MLdatabaseCreator_withOVATION' script, but modified to address
	#             the new problem formulation: 'Given the conditions currently, will scintillation occur in the next T time 
	#             interval'
	#
	# To customize: 
	#      1. Define constants
	# 	   2. Set username and password for CHAIN FTP server
	#      3. Change local directory in which to save the data
	#      4. Change 'start_datetime' and 'days' variables 
	#      5. Make sure CHAIN_stations_PolaRxSonly.xlsx, CHAIN_stations_GSV4004B.xlsx, and CHAIN_data_labels.xlsx are 
        #           in local directory (or directory is given that points to the files)
	# 				NOTE: manually changed the labels that had unicode 'utf-8' special 
	# 					  characters from hard-coded spaces in columns with single digit 
	# 					  numbers (e.g., 1-second phase sigma). This solved problem of writing
	# 					  the DataFrame column titles to csv
	# 	   6. Check that '# Drop unneeded columns' is appropriate (only dropping data that you do not need)
	# 
	# Created from Jupyter notebook on 3.3.2018 and this function input from CHAIN_MLdatabaseCreator_forParallelization_LocalData.py
	# 
	# ----------------------------------------------------------------------------------------
	# ----------------------------------------------------------------------------------------




	
	#-----------------------------------------------------------------------------------------
	# Start timer
	start_timer = time.time()
	#-----------------------------------------------------------------------------------------
	



	#-----------------------------------------------------------------------------------------
	# Define constants
	#el_thresh = 30.
	el_thresh = 15.
	L1_locktime_thresh = 200.;
	IPP_alt_forRange = 110.;
	a = 0.5; 
	b = 0.9; # for discussion of this choice see page 4 of https://www.ann-geophys.net/27/3429/2009/angeo-27-3429-2009.pdf 
	R_earth = 6378.137 # [km]
	leapsecs = 0
	#list prediction hours
	dt_prediction   = [datetime.timedelta( hours=h ) for h in [0.5, 1, 3, 6, 9, 12, 24]]

	# Read CHAIN_station data information 
	local_pwd = '/home/kibrom/kwork/sw-GNSS/fdl18_Frontiers/GNSS_data_derived_products'
	CHAIN_stations = pd.read_excel(local_pwd + '/CHAIN_stations_PolaRxSonly.xlsx').to_dict()

	# Generate x,y,z coordinates of each station in geographic coordinates 
	#   (done once and for all here to avoid repetition in the for loop below)
	list_geox = []
	list_geoy = []
	list_geoz = []
	for k in CHAIN_stations['Lat']:
		#print CHAIN_stations['Lat'][k]
		#print CHAIN_stations['Lon'][k]
	
		# method #1
		#sitex = R_earth * np.cos(np.radians(CHAIN_stations['Lat'][k])) * np.cos(np.radians(CHAIN_stations['Lon'][k]))
		#sitey = R_earth * np.cos(np.radians(CHAIN_stations['Lat'][k])) * np.sin(np.radians(CHAIN_stations['Lon'][k]))
		#sitez = R_earth * np.sin(np.radians(CHAIN_stations['Lat'][k]))
	
		# method #2
		sitex,sitey,sitez = llh2ecef(CHAIN_stations['Lat'][k],CHAIN_stations['Lon'][k],0.) # returned in km

		list_geox.append(sitex)
		list_geoy.append(sitey)
		list_geoz.append(sitez)
		#print sitex,sitey,sitez
	

	CHAIN_stations['geo_x'] = list_geox
	CHAIN_stations['geo_y'] = list_geoy
	CHAIN_stations['geo_z'] = list_geoz


	#-----------------------------------------------------------------------------------------
	#-----------------------------------------------------------------------------------------
	#-----------------------------------------------------------------------------------------
	#-----------------------------------------------------------------------------------------
	#### PolaRxS ####

	# First read the excel files with the CHAIN data labels
	#df_labels_PolaRxS = pd.read_excel('/Users/ryanmc/Documents/TEC_data/CHAIN_data_labels.xlsx', sheet_name='PolaRxS_labels', header=None, usecols=[1])
	df_labels_PolaRxS = pd.read_excel(local_pwd + '/CHAIN_data_labels.xlsx', sheet_name='PolaRxS_labels', header=None, usecols=[1])

	thisdatetime = input_datetime #datetime.datetime(2015,1,1)
	#print('thisdatetime = {0}'.format(thisdatetime))
	thisdoy = thisdatetime.timetuple().tm_yday               
	thisyr = thisdatetime.year
	thisdy = thisdatetime.day
	#print('thisdy = {0}'.format(thisdy))
	thismon = thisdatetime.month

	jan1_datetime = datetime.datetime(thisyr,1,1)
	level2_dir = local_pwd + '/level2/'

	if not os.path.exists(level2_dir):
		os.makedirs(level2_dir)

	#Preprocess time
	preprocess_time = level2_dir + 'ml_db_generator_runtimes/'

	if not os.path.exists(preprocess_time):
		os.makedirs(preprocess_time)



#	filename_check = level2_dir + '/level2/ml_database__' + format(thisyr,'04') + '_' + format(thisdoy,'03') + '.csv'
#	if os.path.isfile(filename_check):
#	print('The data for this doy is available at: ' )
#	print( filename_check)
#		sys.exit('next doy or bye')

	# Initialize the daily dataframe
	df_full_grouped_med = pd.DataFrame()

	# Define local data directory
	local_dir = local_pwd + '/level1/'

	# This will be used if looking ahead one hour
	nextdatetime = thisdatetime + datetime.timedelta(days=1)
	nextyr = nextdatetime.year
	nextdoy = nextdatetime.timetuple().tm_yday
	nextdy = nextdatetime.day
	nextmon = nextdatetime.month
	nexthr = nextdatetime.hour
	#

	thisfile = local_dir + 'PolaRxS_CHAINdata__'  + '{:04}'.format(int(thisyr)) + '_' + '{:03}'.format(int(thisdoy)) + '.csv'
	nextfile = local_dir + 'PolaRxS_CHAINdata__'  + '{:04}'.format(int(nextyr)) + '_' + '{:03}'.format(int(nextdoy)) + '.csv'
	#
	# Read in data to pandas dataframes and concatenate
	if ( os.path.isfile(thisfile) ): 
		try:
			df_thisfile = pd.read_csv(thisfile,index_col=0)
		except:
			print('-------\ncannot read file {0}\n-------'.format(thisfile))
			return
	else: 
		print('-------\n{0} file does not exist\n-------'.format(thisfile))
		return
  
	if ( os.path.isfile(nextfile) ): 
		try:
			df_nextfile = pd.read_csv(nextfile,index_col=0)
		except:
			print('-------\ncannot read file {0}\n-------'.format(nextfile))
			return
	else: 
		print('-------\n{0} file does not exist\n-------'.format(nextfile))
		return

	unique_stations_thisfile = df_thisfile['CHAIN station'].unique()
	unique_stations_nextfile = df_nextfile['CHAIN station'].unique()

	# concatenate dataframes
	df_full = pd.concat( [df_thisfile,df_nextfile] )
	df_full = df_full.reset_index(drop=True)

	# Drop unneeded columns
	drop_col_idxs = [25]
	for n in range(31,59):
		drop_col_idxs.append(n)
	df_full.drop( df_full.columns[drop_col_idxs] ,axis=1)

	# Quality control the data (applying these filters to the scintillation data at current and future times (e.g., t and t+dt))
	#    Filter for elevation angle
	column_idxs_to_set_to_nan = np.arange(4,len(df_full.columns)-1)

	#30 degrees elevation threshold is used here. (Lower elevations can be tested e.g. https://www.ann-geophys.net/27/3429/2009/angeo-27-3429-2009.pdf uses 15 degrees )	
	df_full.loc[ df_full['Elevation (degrees)']<el_thresh, df_full.columns[column_idxs_to_set_to_nan] ] = np.nan

	#    Filter for low lock times - ONLY USING L1 DATA AT THIS TIME 10.27.2017 RMM
	df_full.loc[ df_full['Sig1 lock time (seconds)']<=L1_locktime_thresh,df_full.columns[column_idxs_to_set_to_nan] ] = np.nan

	#    Using L2 or L5 data can be tested

	# Calculate UTC values and update the dataframes
	thesedays, utc = GPS_weekseconds_to_utc(df_full['WN, GPS Week Number'],df_full['TOW, GPS Time of Week (seconds)'],leapsecs)
	
	# NOTE: GPS_weekseconds_to_utc does not retain the years of the input data, so 'doys' will refer to the input year
	#        This causes an issue for the final doy (Dec. 31) where the code below will look 'forward' to Jan. 1 of the 
	#        input year and will not calculate the predicted values for this date. Not fixed. RMM 3.15.2018
	df_full['doys'] = pd.Series( [int(thesedays[i]) for i in range(len(thesedays))] )
	df_full['utc'] = pd.Series( [int(utc[i]) for i in range(len(utc))] ) # need the int() likely since we will make an index of 'utc' later

	# Calculate values projected to the vertical
	VTEC = np.array( df_full['TEC at TOW (TECU), taking calibration into account (see -C option)'] * np.sqrt( 1 - ( np.cos(np.radians(df_full['Elevation (degrees)']))**2 )*((R_earth**2)/((R_earth+IPP_alt_forRange)**2)) ) )
	S4_projected = df_full['Total S4 on Sig1 (dimensionless)'] * ( np.sin(np.radians(df_full['Elevation (degrees)']))**b ) # L1 PHI60
	sigmaPhi_projected = df_full['Phi60 on Sig1, 60-second phase sigma (radians)'] * ( np.sin(np.radians(df_full['Elevation (degrees)']))**a ) # L1 PHI60

	# Add all variables that we wish to calculate a nanmedian value for before GroupBy process
	df_full['VTEC'] = pd.Series( VTEC )
	df_full['S4_projected'] = pd.Series( S4_projected ) 
	df_full['sigmaPhi_projected'] = pd.Series( sigmaPhi_projected ) # need the int() likely since we will make an index of 'utc' later

	# Calculate median values for each time
	df_full_grouped = df_full.groupby(['CHAIN station', 'doys', 'utc'], as_index=True)
	df_full_grouped_med = df_full_grouped.aggregate(np.nanmedian)
	df_full_grouped_med.reset_index(inplace=True) 

	# construct datetimes list
	df_full_grouped_med['datetimes'] = pd.Series( [ jan1_datetime + datetime.timedelta(days=np.float64((df_full_grouped_med['doys'][i]-1)),seconds=np.float64(df_full_grouped_med['utc'][i])) for i in range(len(df_full_grouped_med['doys'])) ] )

	#----------------------------------------------------------------------------------------------------#


	# initialize
	thisdatetime_epoch = []
	thisutc_epoch = []
	thisdoy_epoch = []
	thisaz_epoch = []
	thisel_epoch = []

	thisstation_epoch = []
	thismodel_epoch = [] 
	thislat_epoch = [] 
	thislon_epoch = []
	thisID_epoch = []

	thisTEC_epoch = [] 
	thisdTEC_epoch = [] 
	thisSI_epoch = [] 
	thisSpectralSlope_epoch = []
	thisS4_epoch = [] 
	thisS4projected_epoch = [] 
	thisSigmaPhi_epoch = []
	thisSigmaPhiprojected_epoch = [] 

	futuredatetime_epoch = {}
	futureutc_epoch = {} 
	futuredoy_epoch = {} 
	futureTEC_epoch = {} 
	futuredTEC_epoch = {} 
	futureS4_epoch = {}
	futureS4projected_epoch = {}
	futuresigmaPhi_epoch = {} 
	futuresigmaPhiprojected_epoch = {} 

	# NOTE: we only need to calculate these things for the current day ('now' in the predictive formulation) and 
	#         not for the future day ('prediction time' in the predictive formulation)
	ctr = 0
	for u in range(len(df_full_grouped_med)):
	
		#thisdatetime_epoch_tmp = thisdatetime + datetime.timedelta( days=df_full_grouped_med['doys'][u]-1,seconds=df_full_grouped_med['utc'][u] )
		thisdatetime_epoch_tmp = df_full_grouped_med['datetimes'][u]
		#print('thisdatetime_epoch_tmp = {0}'.format(thisdatetime_epoch_tmp))
	
			# NOTE: we must subtract 1 because 1 refers to the current day in the way that we are calculated days of the year
		thisyr_tmp = thisdatetime_epoch_tmp.year
		thismon_tmp = thisdatetime_epoch_tmp.month
		thisdy_tmp = thisdatetime_epoch_tmp.day
		
		# Skipping these calculations for future days
		if thisdy_tmp != thisdy:
			print('\n----------------- skipping {0}-----------------\n'.format(thisdatetime_epoch_tmp))
			continue

		#thisdatetime_epoch.append( thisdatetime + datetime.timedelta( days=df_full_grouped_med['doys'][u]-1,seconds=df_full_grouped_med['utc'][u] ) )
		thisdatetime_epoch.append( thisdatetime_epoch_tmp )
	   
		thisutc_epoch.append( df_full_grouped_med['utc'][u] )
		thisdoy_epoch.append( df_full_grouped_med['doys'][u] )
		thisstation_epoch.append( df_full_grouped_med['CHAIN station'][u] )
		thisaz_epoch.append( df_full_grouped_med['Azimuth (degrees)'][u] )
		thisel_epoch.append( df_full_grouped_med['Elevation (degrees)'][u] )

		#print(thisdatetime_epoch[ctr])
		#print( df_full_grouped_med['CHAIN station'][u] )
		if u > 0:
			if df_full_grouped_med['CHAIN station'][u] != df_full_grouped_med['CHAIN station'][u-1]:
				print('CHAIN station[{0}] = {1}'.format(u,df_full_grouped_med['CHAIN station'][u]))
			
		for elem in CHAIN_stations['Abbr'].items():
			if elem[1] == df_full_grouped_med['CHAIN station'][u]:
				thismodel_epoch.append( CHAIN_stations['Model'][elem[0]] )
				thislat_epoch.append( CHAIN_stations['Lat'][elem[0]] )
				thislon_epoch.append( CHAIN_stations['Lon'][elem[0]] - 360 )
				thisID_epoch.append( CHAIN_stations['ID'][elem[0]] )
	
		# Save current time variables
		thisTEC_epoch.append( df_full_grouped_med['TEC at TOW (TECU), taking calibration into account (see -C option)'][u] )
		thisdTEC_epoch.append( df_full_grouped_med['dTEC from TOW-15s to TOW (TECU)'][u] )
		thisSI_epoch.append( df_full_grouped_med['SI Index on Sig1: (10*log10(Pmax)-10*log10(Pmin))/(10*log10(Pmax)+10*log10(Pmin)) (dimensionless)'][u] )
		thisSpectralSlope_epoch.append( df_full_grouped_med['p on Sig1, spectral slope of detrended phase in the 0.1 to 25Hz range (dimensionless)'][u] )
		thisS4_epoch.append( df_full_grouped_med['Total S4 on Sig1 (dimensionless)'][u] )
		thisS4projected_epoch.append( df_full_grouped_med['S4_projected'][u] )
		thisSigmaPhi_epoch.append( df_full_grouped_med['Phi60 on Sig1, 60-second phase sigma (radians)'][u] )
		thisSigmaPhiprojected_epoch.append( df_full_grouped_med['sigmaPhi_projected'][u] )
		# Assign the future values to the appropriate times
	#     future_datetime_epoch_tmp = thisdatetime_epoch[u] + datetime.timedelta( hours=1 )

		future_datetime_epoch_tmp = df_full_grouped_med['datetimes'][u] + dt_prediction [0] 
		future_datetime_epoch_tmp1 = df_full_grouped_med['datetimes'][u] + dt_prediction [1] 
		future_datetime_epoch_tmp2 = df_full_grouped_med['datetimes'][u] + dt_prediction [2] 
		future_datetime_epoch_tmp3 = df_full_grouped_med['datetimes'][u] + dt_prediction [3] 
		future_datetime_epoch_tmp4 = df_full_grouped_med['datetimes'][u] + dt_prediction [4] 
		future_datetime_epoch_tmp5 = df_full_grouped_med['datetimes'][u] + dt_prediction [5]
		future_datetime_epoch_tmp6 = df_full_grouped_med['datetimes'][u] + dt_prediction [6]
			#print (future_datetime_epoch_tmp )
		#print(future_datetime_epoch_tmp)

		idx_prediction_datetime_tmp = (df_full_grouped_med['datetimes'] == future_datetime_epoch_tmp ) & (df_full_grouped_med['CHAIN station'] == df_full_grouped_med['CHAIN station'][u])
		idx_prediction_datetime_tmp1 = (df_full_grouped_med['datetimes'] == future_datetime_epoch_tmp1 ) & (df_full_grouped_med['CHAIN station'] == df_full_grouped_med['CHAIN station'][u])
		idx_prediction_datetime_tmp2 = (df_full_grouped_med['datetimes'] == future_datetime_epoch_tmp2 ) & (df_full_grouped_med['CHAIN station'] == df_full_grouped_med['CHAIN station'][u])
		idx_prediction_datetime_tmp3 = (df_full_grouped_med['datetimes'] == future_datetime_epoch_tmp3 ) & (df_full_grouped_med['CHAIN station'] == df_full_grouped_med['CHAIN station'][u])
		idx_prediction_datetime_tmp4 = (df_full_grouped_med['datetimes'] == future_datetime_epoch_tmp4 ) & (df_full_grouped_med['CHAIN station'] == df_full_grouped_med['CHAIN station'][u])
		idx_prediction_datetime_tmp5 = (df_full_grouped_med['datetimes'] == future_datetime_epoch_tmp5 ) & (df_full_grouped_med['CHAIN station'] == df_full_grouped_med['CHAIN station'][u])
		idx_prediction_datetime_tmp6 = (df_full_grouped_med['datetimes'] == future_datetime_epoch_tmp6 ) & (df_full_grouped_med['CHAIN station'] == df_full_grouped_med['CHAIN station'][u])

		prediction_times = [idx_prediction_datetime_tmp, idx_prediction_datetime_tmp1, idx_prediction_datetime_tmp2, idx_prediction_datetime_tmp3, idx_prediction_datetime_tmp4, idx_prediction_datetime_tmp5, idx_prediction_datetime_tmp6]

		for p in range(0,7):
			if p not in futuredatetime_epoch:
				futuredatetime_epoch[p] = []
				futureutc_epoch[p] = []
				futuredoy_epoch[p] = []
				futureTEC_epoch[p] = []
				futuredTEC_epoch[p] = []
				futureS4_epoch[p] = []
				futureS4projected_epoch[p] = []
				futuresigmaPhi_epoch[p] = []
				futuresigmaPhiprojected_epoch[p] = []
			if not any( prediction_times [p] ):
				print( 'no Trues in list for station = {0}, doy = {1}, utc = {2}'.format(df_full_grouped_med['CHAIN station'][u],df_full_grouped_med['doys'][u],df_full_grouped_med['utc'][u]) )
				futuredatetime_epoch[p].append(np.nan)
				futureutc_epoch[p].append(np.nan)
				futuredoy_epoch[p].append(np.nan)
				futureTEC_epoch[p].append(np.nan)
				futuredTEC_epoch[p].append(np.nan)
				futureS4_epoch[p].append(np.nan)
				futureS4projected_epoch[p].append(np.nan)
				futuresigmaPhi_epoch[p].append(np.nan)
				futuresigmaPhiprojected_epoch[p].append(np.nan)
		
			elif sum(prediction_times [p])>1:
				print( 'number of matching prediction time entries is > 1, something wrong for doy = {0}, utc = {1}'.format(df_full_grouped_med['doys'][u],df_full_grouped_med['utc'][u]) )	
				futuredatetime_epoch[p].append(np.nan)
				futureutc_epoch[p].append(np.nan)
				futuredoy_epoch[p].append(np.nan)
				futureTEC_epoch[p].append(np.nan)
				futuredTEC_epoch[p].append(np.nan)
				futureS4_epoch[p].append(np.nan)
				futureS4projected_epoch[p].append(np.nan)
				futuresigmaPhi_epoch[p].append(np.nan)
				futuresigmaPhiprojected_epoch[p].append(np.nan)

			else: 
				futuredatetime_epoch[p].append(df_full_grouped_med['datetimes'][prediction_times [p] ].values[0])
				futureutc_epoch[p].append(df_full_grouped_med['utc'][prediction_times [p] ].values[0])
				futuredoy_epoch[p].append(df_full_grouped_med['doys'][prediction_times [p] ].values[0])
				futureTEC_epoch[p].append(df_full_grouped_med['TEC at TOW (TECU), taking calibration into account (see -C option)'][prediction_times [p] ].values[0])
				futuredTEC_epoch[p].append(df_full_grouped_med['dTEC from TOW-15s to TOW (TECU)'][prediction_times [p] ].values[0])
				futureS4_epoch[p].append(df_full_grouped_med['Total S4 on Sig1 (dimensionless)'][prediction_times [p] ].values[0])
				futureS4projected_epoch[p].append(df_full_grouped_med['S4_projected'][prediction_times [p] ].values[0])
				futuresigmaPhi_epoch[p].append(df_full_grouped_med['Phi60 on Sig1, 60-second phase sigma (radians)'][prediction_times [p] ].values[0])
				futuresigmaPhiprojected_epoch[p].append(df_full_grouped_med['sigmaPhi_projected'][prediction_times [p] ].values[0])
		ctr += 1

	#----------------------------------------------------------------------------------------------------#
						#^^^^^^^ Add variables to the dataframe ^^^^^^^#
	#----------------------------------------------------------------------------------------------------#

	# Create ML DB dataframe
	try: 
			df_save = pd.DataFrame()
			df_save['datetime'] = pd.Series( thisdatetime_epoch )
			df_save['doy'] = pd.Series( thisdoy_epoch )
			df_save['ut'] = pd.Series( thisutc_epoch )
			df_save['azimuth [deg]'] = pd.Series( thisaz_epoch )
			df_save['elevation [deg]'] = pd.Series( thisel_epoch )
			df_save['geographic latitude [deg]'] = pd.Series( thislat_epoch )
			df_save['geographic longitude [deg]'] = pd.Series( thislon_epoch )
	#     at current time 
			df_save['TEC at current time [TECU]'] = pd.Series( thisTEC_epoch )
			df_save['dTEC 0min-15s to 0min-0s [TECU]'] = pd.Series( thisdTEC_epoch )
			df_save['SI [dimensionless]'] = pd.Series( thisSI_epoch )
			df_save['spectral slope [dimensionless]'] = pd.Series( thisSpectralSlope_epoch )
			df_save['S4 [dimensionless]'] = pd.Series( thisS4_epoch )
			df_save['S4 projected to vertical [dimensionless]'] = pd.Series( thisS4projected_epoch )
			df_save['sigmaPhi [radians]'] = pd.Series( thisSigmaPhi_epoch )
			df_save['sigmaPhi projected to vertical [radians]'] = pd.Series( thisSigmaPhiprojected_epoch )
	#     at 0.5hr prediction time 
			df_save['datetime at prediction time (.5h)'] = pd.Series( pd.to_datetime(futuredatetime_epoch [0] ) )
			df_save['ut at prediction time(.5h) [sec]'] = pd.Series( futureutc_epoch [0])
			df_save['doy at prediction time(.5h) [sec]'] = pd.Series( futuredoy_epoch [0])
			df_save['TEC at prediction time(.5h) [TECU]'] = pd.Series( futureTEC_epoch [0])
			df_save['dTEC at prediction time(.5h) [TECU]'] = pd.Series( futuredTEC_epoch [0])
			df_save['S4 at prediction time(.5h) [dimensionless]'] = pd.Series( futureS4_epoch  [0])
			df_save['S4 projected to vertical at prediction time(.5h) [dimensionless]'] = pd.Series( futureS4projected_epoch [0])
			df_save['sigmaPhi at prediction time(.5h) [radians]'] = pd.Series( futuresigmaPhi_epoch [0])
			df_save['sigmaPhi projected to vertical at prediction time(.5h) [radians]'] = pd.Series( futuresigmaPhiprojected_epoch [0])
	#     at 1hr prediction time 
			df_save['datetime at prediction time (1h)'] = pd.Series( pd.to_datetime(futuredatetime_epoch [1]) )
			df_save['ut at prediction time(1h) [sec]'] = pd.Series( futureutc_epoch [1])
			df_save['doy at prediction time(1h) [sec]'] = pd.Series( futuredoy_epoch [1])
			df_save['TEC at prediction time(1h) [TECU]'] = pd.Series( futureTEC_epoch [1])
			df_save['dTEC at prediction time(1h) [TECU]'] = pd.Series( futuredTEC_epoch [1])
			df_save['S4 at prediction time(1h) [dimensionless]'] = pd.Series( futureS4_epoch  [1])
			df_save['S4 projected to vertical at prediction time(1h) [dimensionless]'] = pd.Series( futureS4projected_epoch [1])
			df_save['sigmaPhi at prediction time(1h) [radians]'] = pd.Series( futuresigmaPhi_epoch [1])
			df_save['sigmaPhi projected to vertical at prediction time(1h) [radians]'] = pd.Series( futuresigmaPhiprojected_epoch [1])
			print ('at 3h prediction time')
	#     at 3hr prediction time 
			df_save['datetime at prediction time (3h)'] = pd.Series( pd.to_datetime(futuredatetime_epoch [2]) )
			df_save['ut at prediction time(3h) [sec]'] = pd.Series( futureutc_epoch [2])
			df_save['doy at prediction time(3h) [sec]'] = pd.Series( futuredoy_epoch [2])
			df_save['TEC at prediction time(3h) [TECU]'] = pd.Series( futureTEC_epoch [2])
			df_save['dTEC at prediction time(3h) [TECU]'] = pd.Series( futuredTEC_epoch [2])
			df_save['S4 at prediction time(3h) [dimensionless]'] = pd.Series( futureS4_epoch  [2])
			df_save['S4 projected to vertical at prediction time(3h) [dimensionless]'] = pd.Series( futureS4projected_epoch [2])
			df_save['sigmaPhi at prediction time(3h) [radians]'] = pd.Series( futuresigmaPhi_epoch [2])
			df_save['sigmaPhi projected to vertical at prediction time(3h) [radians]'] = pd.Series( futuresigmaPhiprojected_epoch [2])
			print ('at 6h prediction time')
	#     at 6hr prediction time 
			df_save['datetime at prediction time (6h)'] = pd.Series( pd.to_datetime(futuredatetime_epoch [3]) )
			df_save['ut at prediction time(6h) [sec]'] = pd.Series( futureutc_epoch [3])
			df_save['doy at prediction time(6h) [sec]'] = pd.Series( futuredoy_epoch [3])
			df_save['TEC at prediction time(6h) [TECU]'] = pd.Series( futureTEC_epoch [3])
			df_save['dTEC at prediction time(6h) [TECU]'] = pd.Series( futuredTEC_epoch [3])
			df_save['S4 at prediction time(6h) [dimensionless]'] = pd.Series( futureS4_epoch  [3])
			df_save['S4 projected to vertical at prediction time(6h) [dimensionless]'] = pd.Series( futureS4projected_epoch [3])
			df_save['sigmaPhi at prediction time(6h) [radians]'] = pd.Series( futuresigmaPhi_epoch [3])
			df_save['sigmaPhi projected to vertical at prediction time(6h) [radians]'] = pd.Series( futuresigmaPhiprojected_epoch [3])
			print ('at 9hr prediction time')
	#     at 9hr prediction time 
			df_save['datetime at prediction time (9h)'] = pd.Series( pd.to_datetime(futuredatetime_epoch [4]) )
			df_save['ut at prediction time(9h) [sec]'] = pd.Series( futureutc_epoch [4])
			df_save['doy at prediction time(9h) [sec]'] = pd.Series( futuredoy_epoch [4])
			df_save['TEC at prediction time(9h) [TECU]'] = pd.Series( futureTEC_epoch [4])
			df_save['dTEC at prediction time(9h) [TECU]'] = pd.Series( futuredTEC_epoch [4])
			df_save['S4 at prediction time(9h) [dimensionless]'] = pd.Series( futureS4_epoch  [4])
			df_save['S4 projected to vertical at prediction time(9h) [dimensionless]'] = pd.Series( futureS4projected_epoch [4])
			df_save['sigmaPhi at prediction time(9h) [radians]'] = pd.Series( futuresigmaPhi_epoch [4])
			df_save['sigmaPhi projected to vertical at prediction time(9h) [radians]'] = pd.Series( futuresigmaPhiprojected_epoch [4])
			print ('at 12r prediction time')
	#     at 12hr prediction time 
			df_save['datetime at prediction time (12h)'] = pd.Series( pd.to_datetime(futuredatetime_epoch [5]) )
			df_save['ut at prediction time(12h) [sec]'] = pd.Series( futureutc_epoch [5])
			df_save['doy at prediction time(12h) [sec]'] = pd.Series( futuredoy_epoch [5])
			df_save['TEC at prediction time(12h) [TECU]'] = pd.Series( futureTEC_epoch [5])
			df_save['dTEC at prediction time(12h) [TECU]'] = pd.Series( futuredTEC_epoch [5])
			df_save['S4 at prediction time(12h) [dimensionless]'] = pd.Series( futureS4_epoch  [5])
			df_save['S4 projected to vertical at prediction time(12h) [dimensionless]'] = pd.Series( futureS4projected_epoch [5])
			df_save['sigmaPhi at prediction time(12h) [radians]'] = pd.Series( futuresigmaPhi_epoch [5])
			df_save['sigmaPhi projected to vertical at prediction time(12h) [radians]'] = pd.Series( futuresigmaPhiprojected_epoch [5])
			print ('at 12r prediction time')
	#     at 24hr prediction time 
			df_save['datetime at prediction time (24h)'] = pd.Series( pd.to_datetime(futuredatetime_epoch [6]) )
			df_save['ut at prediction time(24h) [sec]'] = pd.Series( futureutc_epoch [6])
			df_save['doy at prediction time(24h) [sec]'] = pd.Series( futuredoy_epoch [6])
			df_save['TEC at prediction time(24h) [TECU]'] = pd.Series( futureTEC_epoch [6])
			df_save['dTEC at prediction time(24h) [TECU]'] = pd.Series( futuredTEC_epoch [6])
			df_save['S4 at prediction time(24h) [dimensionless]'] = pd.Series( futureS4_epoch  [6])
			df_save['S4 projected to vertical at prediction time(24h) [dimensionless]'] = pd.Series( futureS4projected_epoch [6])
			df_save['sigmaPhi at prediction time(24h) [radians]'] = pd.Series( futuresigmaPhi_epoch [6])
			df_save['sigmaPhi projected to vertical at prediction time(24h) [radians]'] = pd.Series( futuresigmaPhiprojected_epoch [6])
			print ('at 24h prediction time')		
	#
			df_save['CHAIN station'] = pd.Series( thisstation_epoch )
			df_save['CHAIN station model'] = pd.Series( thismodel_epoch )
			df_save['CHAIN station ID number'] = pd.Series( thisID_epoch )
			print ('at 24hr prediction time')
			filename_save = level2_dir + 'ml_database__' + format(thisyr,'04') + '_' + format(thisdoy,'03') + '.csv'
			pd.DataFrame.to_csv(df_save,filename_save,na_rep='NaN')
			del filename_save 
	
	except Exception as e:
		print('\n--------------\n ***unable to save dataframe for doy = {0} ***\n--------------\n'.format(thisdoy))
		print('\n--------------\n ***with error = {0} ***\n--------------\n'.format(e))
		
		print('\n--------------\n ***returning without saving...fin ***\n--------------\n')
		
		return



	print('fin')

	#-----------------------------------------------------------------------------------------
	# End timer and save time to text file
	end_timer = time.time()
	runtime_thisday = end_timer - start_timer
	np.savetxt(preprocess_time + 'runtime__' + format(thisyr,'04') + '_' + format(thisdoy,'03') + '.txt', np.array(runtime_thisday).reshape(1,),fmt='%.2f')
	#-----------------------------------------------------------------------------------------


def main():

	# note: sys.argv[0] is the script name
	numprocessors = int(sys.argv[1])
	print('numprocessors = {0}'.format(numprocessors))

	pool = multiprocessing.Pool(numprocessors)
	datetime_start = datetime.datetime(2017,1,1)
	input_datetimes = [ (datetime_start + datetime.timedelta(days=d)) for d in (range(1)) ]
	pool.map(PolaRxS_MLDatabaseGeneration,input_datetimes) 


if __name__=='__main__':
	main()
