import numpy as np
import pandas as pd
import datetime
from geospacepy import omnireader
from sunpy.net import hek, Fido, attrs as a
from sunpy.timeseries import TimeSeries
import matplotlib.pyplot as plt 
import csv

def download_omni_text(input_datetime, file_path=os.getwcd()+'/omni_data/'):
    """
    Function to download solar OMNI data for the day of interest.
    This includes the the basic OMNI data of solar wind and geomagnetic 
    parameters as well as calculating the Newell and Borovsky coupling 
    constants, and the GOES XRS X-ray flux. 

    This function downloads the data, resamples to 1 minute, and also 
    calculates the previous inputs at 15 and 30 minutes previously.
  

    Parameters
    ----------
    input_datetime: ~`datetime.datetime`
        the input date day for which you want the data.

    file_path: `~str`, optional
        path for which to save the csv of data to. If not provided
        a directory named omni_data will be created in current working
        directory and data files will be populated there.

    Notes
    -----
    This function will save a csv file with the omni solar data
    for the input day in the format `solardata_yyyy_doy.csv`.

    Examples
    --------
    >>> import datetime
    >>> t_input = datetime.datetime(2015,1,1)
    >>> download_omni_text(t_input, file_path='/Users/laurahayes/FDL/solar_data/')

    """

    t_start = input_datetime - datetime.timedelta(1)
    t_end = input_datetime + datetime.timedelta(1) + datetime.timedelta(minutes = 10)

    t_start_day = input_datetime
    t_end_day = input_datetime + datetime.timedelta(minutes = 1439) 

    #--------------------------------------------------------#
    #   OMNI Data - includes solar wind, and geomag params   #
    #--------------------------------------------------------#

    #get OMNI data
    omniInt = omnireader.omni_interval(t_start,t_end,'5min', cdf_or_txt = 'txt')
    
    #print(omniInt.cdfs[0].vars) #prints all the variables available on omni

    epochs = omniInt['Epoch'] #time array for omni 5min data
    By,Bz,AE,SymH = omniInt['BY_GSM'],omniInt['BZ_GSM'],omniInt['AE_INDEX'], omniInt['SYM_H']
    vsw,psw = omniInt['flow_speed'], omniInt['Pressure']
    borovsky_reader = omnireader.borovsky(omniInt)
    borovsky = borovsky_reader()


    def NewellCF_calc(v,bz,by):
        # v expected in km/s
        # b's expected in nT    
        NCF = np.zeros_like(v)
        NCF.fill(np.nan)
        bt = np.sqrt(by**2 + bz**2)
        bztemp = bz
        bztemp[bz == 0] = .001
        #Caculate clock angle (theta_c = t_c)
        tc = np.arctan2(by,bztemp)
        neg_tc = bt*np.cos(tc)*bz < 0 
        tc[neg_tc] = tc[neg_tc] + np.pi
        sintc = np.abs(np.sin(tc/2.))
        NCF = (v**1.33333)*(sintc**2.66667)*(bt**0.66667)
        return NCF

    def borovsky_fix(oi):

        (densvar,vswvar) = ('proton_density','flow_speed')
        bx,by,bz = oi['BX_GSE'],oi['BY_GSM'],oi['BZ_GSM']
        n,vsw,mach = oi[densvar],oi[vswvar],oi['Mach_num']
        bt = np.sqrt(by**2+bz**2)
        #Compute IMF clock angle
        ca = np.arctan2(by,bz)
        borovsky = 3.29e-2*(np.sin(ca/2)**2)*np.sqrt(n)*vsw**2*mach**(-.18)*np.exp(np.sqrt((mach/3.42)))
        self.varvals=borovsky
        return borovsky

    newell = NewellCF_calc(vsw, Bz, By)

    #calculate clock angle
    clock_angle = np.degrees(np.arctan2( By,Bz))
    clock_angle[clock_angle < 0] = clock_angle[clock_angle<0] + 360.

    print('Got 5 minutes data')

    omniInt_1hr = omnireader.omni_interval(t_start,t_end,'hourly', cdf_or_txt = 'txt')
    proton_flux_10MeV, proton_flux_30MeV, proton_flux_60MeV = omniInt_1hr['PR-FLX_10'], omniInt_1hr['PR-FLX_30'], omniInt_1hr['PR-FLX_60']
    epochs_1hr = omniInt_1hr['Epoch'] #datetime timestamps
    F107,KP = omniInt_1hr['F10_INDEX'],omniInt_1hr['KP']


    print('Got hour data')
    #--------------------------------------------------------#
    #   GOES X-ray data - Channel 1-8A, defines flare class  #
    #--------------------------------------------------------#


    results = Fido.search(a.Time(t_start, t_end), a.Instrument('XRS'))
    files = Fido.fetch(results)
    goes = TimeSeries(files, concatenate = True)

    goes_l = goes.data['xrsb']
    
    print('Got GOES data')
    #--------------------------------------------------------#
    #   Resample data to 1min to match GNSS CHAIN network    #
    #--------------------------------------------------------#


    #resample OMNI Solar Wind Data
    By_data = pd.Series(By, index = epochs).resample('1T').pad().truncate(t_start_day, t_end_day)
    Bz_data = pd.Series(Bz, index = epochs).resample('1T').pad().truncate(t_start_day, t_end_day)
    AE_data = pd.Series(AE, index = epochs).resample('1T').pad().truncate(t_start_day, t_end_day)
    SymH_data = pd.Series(SymH, index = epochs).resample('1T').pad().truncate(t_start_day, t_end_day)
    vsw_data = pd.Series(vsw, index = epochs).resample('1T').pad().truncate(t_start_day, t_end_day)
    psw_data = pd.Series(psw, index = epochs).resample('1T').pad().truncate(t_start_day, t_end_day)
    borovsky_data = pd.Series(borovsky, index = epochs).resample('1T').pad().truncate(t_start_day, t_end_day)
    newell_data = pd.Series(newell, index = epochs).resample('1T').pad().truncate(t_start_day, t_end_day)
    proton_10_data = pd.Series(proton_flux_10MeV, index = epochs_1hr).resample('1T').pad().truncate(t_start_day, t_end_day)
    proton_30_data = pd.Series(proton_flux_30MeV, index = epochs_1hr).resample('1T').pad().truncate(t_start_day, t_end_day)
    proton_60_data = pd.Series(proton_flux_60MeV, index = epochs_1hr).resample('1T').pad().truncate(t_start_day, t_end_day)
    clock_angle_data = pd.Series(clock_angle, index = epochs).resample('1T').pad().truncate(t_start_day, t_end_day)

    F107data = pd.Series(F107, index = epochs_1hr).resample('1T').pad().truncate(t_start_day, t_end_day)
    KPdata = pd.Series(KP, index = epochs_1hr).resample('1T').pad().truncate(t_start_day, t_end_day)


    #function to find data at previous time intervals
    def roll_back(data, minutes = 1):
        ts = t_start_day - datetime.timedelta(minutes = minutes)
        te = t_end_day - datetime.timedelta(minutes = minutes)
        data = pd.Series(data, index = epochs).resample('1T').pad()
        new_data = data.truncate(ts, te)
        rolled_data = pd.Series(np.array(new_data), index = By_data.index)
        return rolled_data

    #calculate rolled back timeseries - 15 and 30 minutes previous
    By_15 = roll_back(By, minutes = 15)
    By_30 = roll_back(By, minutes = 30)
    Bz_15 = roll_back(Bz, minutes = 15)
    Bz_30 = roll_back(Bz, minutes = 30)
    AE_15 = roll_back(AE, minutes = 15)
    AE_30 = roll_back(AE, minutes = 30)
    SymH_15 = roll_back(SymH, minutes = 15)
    SymH_30 = roll_back(SymH, minutes = 30)
    vsw_15 = roll_back(vsw, minutes = 15)
    vsw_30 = roll_back(vsw, minutes = 30)
    psw_15 = roll_back(psw, minutes = 15)
    psw_30 = roll_back(psw, minutes = 30)   
    borovsky_15 = roll_back(borovsky, minutes = 15)
    borovsky_30 = roll_back(borovsky, minutes = 30)
    newell_15 = roll_back(newell, minutes = 15)
    newell_30 = roll_back(newell, minutes = 30)
    clock_angle_15 = roll_back(clock_angle, minutes = 15)
    clock_angle_30 = roll_back(clock_angle, minutes = 30)
    
    #resample GOES X-ray flux
    goes_data = goes_l.resample('1T').mean().truncate(t_start_day, t_end_day)

    #put all in a dataframe and save
    dataframe = pd.DataFrame()
    dataframe['Bz - 0min [nT]'] = Bz_data
    dataframe['Bz - 15min [nT]'] = Bz_15
    dataframe['Bz - 30min [nT]'] = Bz_30

    dataframe['By - 0min [nT]'] = By_data
    dataframe['By - 15min [nT]'] = By_15
    dataframe['By - 30min [nT]'] = By_30

    dataframe['Vsw - 0min [km/s]'] = vsw_data
    dataframe['Vsw - 15min [km/s]'] = vsw_15
    dataframe['Vsw - 30min [km/s]'] = vsw_30

    dataframe['Psw - 0min [nPa]'] = psw_data
    dataframe['Psw - 15min [nPa]'] = psw_15
    dataframe['Psw - 30min [nPa]'] = psw_30

    dataframe['AE - 0min [nT]'] = AE_data
    dataframe['AE - 15min [nT]'] = AE_15
    dataframe['AE - 30min [nT]'] = AE_30

    dataframe['SymH - 0min [nT]'] = SymH_data
    dataframe['SymH - 15min [nT]'] = SymH_15
    dataframe['SymH - 30min [nT]'] = SymH_30


    dataframe['Clock Angle - 0min [deg]'] = clock_angle_data
    dataframe['Clock Angle - 15min [deg]'] = clock_angle_15
    dataframe['Clock Angle - 30min [deg]'] = clock_angle_30

    dataframe['Newell CF - 0min [m/s^(4/3) T^(2/3)]'] = newell_data
    dataframe['Newell CF - 15min [m/s^(4/3) T^(2/3)]'] = newell_15
    dataframe['Newell CF - 30min [m/s^(4/3) T^(2/3)]'] = newell_30

    dataframe['Borovsky CF - 0min [nT km/s]'] = borovsky_data
    dataframe['Borovsky CF - 15min [nT km/s]'] = borovsky_15
    dataframe['Borovsky CF - 30min [nT km/s]'] = borovsky_30

    dataframe['Kp [dimensionless]'] = KPdata
    dataframe['F107 [sfu=10^-22 W/m^2/hz]'] = F107data


    dataframe['Proton 10MeV'] = proton_10_data
    dataframe['Proton 30MeV'] = proton_30_data
    dataframe['Proton 60MeV'] = proton_60_data

    dataframe['GOES X-ray Wm^-2'] = goes_data
    dataframe_nan = dataframe.replace(9999.99, np.nan) #replace 9999.99 with nans

    filepath = file_path
    if not os.path.exists(filepath):
        os.mkdir(filepath)
    filename = filepath + 'solardata' + input_datetime.strftime('%Y') + '_' +input_datetime.strftime('%j') + '.csv'
    dataframe_nan.to_csv(filename, index_label = 'Datetime')
    print('saved')
    print(input_datetime.strftime('%j'))




def main():
    """
    Download data for 2015
  
    If a specific day for downloading doesn't work it writes the date that failed
    in a csv file.
    """

  t_start = datetime.datetime(2015,1,1)
  dates_to_get = [t_start + datetime.timedelta(d) for d in range(365)]
  for i in range(0, len(dates_to_get)):
      try:
          download_omni_text(dates_to_get[i], file_path='/Users/laurahayes/FDL/solar_data/')
          print(i)
      except:
          data_to_write = [str(dates_to_get[i])]
          with open('/Users/laurahayes/FDL/DATA_LOCAL_OMNI/2015_solar_txt/failed_file.txt', 'a') as outfile:

              writer = csv.writer(outfile)
              writer.writerow(str(data_to_write))

if __name__=='__main__':
  main()
