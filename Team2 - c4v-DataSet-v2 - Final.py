# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 10:57:18 2020

@author: mpala
"""
# =============================================================================
# # Import Packages 
# =============================================================================
from google.cloud import bigquery
import os
import pandas as pd
import urllib.request
import numpy as np
import datetime 

# =============================================================================
# #Define Functions
# =============================================================================
def weighted_rolling_sum(data, column, alpha = 0.12, weeks = 8):
    """
    PARAMETERS
    -------------
    data   :  The dataframe where the column is.
    column :  A list of columns to take calculate over.
    alpha  :  The rate of deprecation.
    weeks  :  How many weeks to include in the calculation.
    By weighted rolling sum we give a higher value to newer values in the dataset.
    """
    for c in range(len(column)):
        x = data.loc[len(data.index)-1,'week']
        y = data.loc[len(data.index)-1,'year']
        n = 0
        i = 0
        wrs = 0
        for i in range(weeks):
            if x == 1:
                wrs = wrs + np.mean(data[column[c]][data.loc[:,'week']==x][data.loc[:,'year'] == y])*(1-(alpha*n)**2)
                x = 52
                y = y - 1
                n = n + 1
                i = i + 1
            else:
                wrs = wrs + np.mean(data[column[c]][data.loc[:,'week']==x][data.loc[:,'year'] == y])*(1-(alpha*n)**2)
                x = x - 1
                n = n + 1
                i = i + 1
        # add wrs to column
        data['wrs_'+str(column[c])]= wrs

# =============================================================================
# Import the Data
# =============================================================================
url = 'https://storage.googleapis.com/angostura-public/hult-hackathon-key.json'
urllib.request.urlretrieve(url, './hult-hackathon-key.json')

# Second, we add the key to our environment

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './hult-hackathon-key.json'

client = bigquery.Client()

# We can dump the BigQuery results directly into a DataFrame
QUERY = ('select * from `angostura_dev`.eh_health_survey_response')

df = client.query(QUERY).to_dataframe()


##############################################################################
##################### Operations #################################################

# Data Cleaning for Operations

# Operational Variables (Beds, Electricity, Water..)
vars_ops = ['timestamp', 'hospital_code',
            'federal_entity', 'arch_beds_count',
            'op_beds_count', 'op_beds_er_count',
            'op_pavilions_count',
            'wash_failure_icu','wash_failure_er',
            'wahs_failure_sx','power_outage',
            'power_outage_avg_failures_per_day',
            'power_outage_days_count', 'power_outage_avg_duration',
            'power_outage_equipment_failure','power_outage_equipment_failure_specify',
            'power_generator_available','power_outage_mortatility',
            'power_outage_deaths_count']

df_ops = df[vars_ops]

#Change types, extract dates and hours, replace responses to ordinal numbers
df_ops['timestamp'] = df_ops['timestamp'].astype('datetime64').dt.strftime('%Y-%m-%d')

df_ops['power_outage_avg_duration'] = df_ops['power_outage_avg_duration'].astype('datetime64').dt.strftime('%H').astype('int64')
df_ops['op_pavilions_count'] = df_ops['op_pavilions_count'].astype('int64')
df_ops = df_ops.replace({
                        'no hubo agua ningún dia'                          : "no water", 
                        '3 a 5 días , sin soporte  alterno'                : "no water",
                        '< 3 días, sin soporte alterno (cisternas)'        : "no water",
                        '3 a 5 días, con soporte alterno'                  : "using reserve", 
                        '< 3 días, con soporte alterno'                    : "using reserve",
                        'hubo agua todos los días'                         : "water everyday",
                        'si'                                               : "yes",
                        'sí'                                               : "yes",
                        'no, si'                                           : "no", 
                        'si, no'                                           : "no",
                        'entre 3 y 5 días'                                 : "between 3 and 5 days", 
                        'menos de 3 días'                                  : "less than 3 days", 
                        'todos los días'                                   : "every day",
                        'funciona menos de 3 días'                         : "not working", 
                        'funciona entre 3 y 5 días'                        : "not working",
                        'hay pero no funciona'                             : "not working",  
                        'funciona todos los días'                          : "works every day",
                        'nunca ha habido'                                  : "never existed",
                        "No hubo agua ningún dia"                          : "no water",
                        "Hubo agua todos los días"                         : "water everyday"
                        })
    
    
df_ops = df_ops.fillna(" ")
df_ops['CHECK'] = 1
operation=df_ops


#Joining the Name of the Hospitals to the Operations Data Set
hospital_names = pd.read_excel("Hospital_Names.xlsx")
operation= operation.merge(hospital_names, on='hospital_code', how='left') 

##############################################################################
##################### Staff #################################################

#Data Cleaning for Staff
values_dict = {
    'Nunca ha existido'  : 5,
    'No hubo'            : 5,
    'No operativa'       : 4,
    'Entre 1 y 2 días'   : 3,
    'Menos de 3 de días' : 2,
    'Entre 3 y 5 días'   : 1,
    'Todos los días'     : 0
}

staff = ['timestamp', 'hospital_code',
         'federal_entity',
        'er_staff_residents_and_rural_day_on_call',
         'er_staff_specialist_day_on_call',
         'er_staff_mic_day_on_call',
         'er_staff_nurse_day_on_call',
         'er_staff_non_professional_nurse_day_on_call',
         'er_staff_residents_and_rural_night_on_call',
         'er_staff_specialist_night_on_call',
         'er_staff_mic_night_on_call',
         'er_staff_nurse_night_on_call',
         'er_staff_non_professional_nurse_night_on_call'
        ]

df2 = df[staff]

df3=df2.replace(values_dict)

dicti= {'Robos , hurtos o disparos dentro del centro asistencial.':1, 'Violencia contra personal de hospital por familiares':1,
       'Violencia contra personal de hospital por familiares, Robos , hurtos o disparos dentro del centro asistencial.':1,
       'Violencia contra personal de hospital por familiares, Violencia contra personal por  grupos paramilitares .':1,
       'Violencia contra personal de hospital por familiares, Violencia contra personal por  grupos paramilitares ., Robos , hurtos o disparos dentro del centro asistencial.':1,
       'Violencia contra personal de hospital por familiares, Violencia contra personal por  grupos paramilitares ., Violencia contra personal por fuerzas de seguridad':1,
       'Violencia contra personal de hospital por familiares, Violencia contra personal por  grupos paramilitares ., Violencia contra personal por fuerzas de seguridad, Robos , hurtos o disparos dentro del centro asistencial.':1,
       'Violencia contra personal de hospital por familiares, Violencia contra personal por fuerzas de seguridad':1,
       'Violencia contra personal de hospital por familiares, Violencia contra personal por fuerzas de seguridad, Robos , hurtos o disparos dentro del centro asistencial.':1,
       'Violencia contra personal por  grupos paramilitares .':1, 'Violencia contra personal por  grupos paramilitares ., Violencia contra personal por fuerzas de seguridad':1,
       'Violencia contra personal por fuerzas de seguridad':1, 'Violencia contra personal por fuerzas de seguridad, Robos , hurtos o disparos dentro del centro asistencial.':1}

df3 = df3.replace(dicti)
df3['timestamp'] = df3['timestamp'].astype('datetime64').dt.strftime('%Y-%m-%d')

df3 = df3.fillna(" ")


##############################################################################
##################### Medical Equipment ######################################

#Creating a copy
df1 = df.copy()


info = df1.loc[:,['timestamp', 'report_week', 'hospital_code', 'federal_entity', 
                  'hospital_type', 'administrative_entity']]
#Opearability data frame
operability = df1.loc[:,['operability_icu','operability_icu_p', 'operability_er', 
                         'operability_sx', 'operability_lab', 'operability_uls', 
                         'operability_ct_mri', 'operability_xr']]

#er equipment data frame
er_equip = df1.loc[:, ['er_avail_defibrillator', 'er_avail_ott_intubation', 
                       'er_avail_catheter','er_avail_oxygen_suction']]

#sx equipment data frame
sx_equip = df1.loc[:,['sx_avail_ott_intubation', 'sx_avail_patient_lingerie_kit', 
                     'sx_avail_disposables_mask_gloves_gown', 'sx_avail_oxygen_suction']]

#Gather the seperated data frames
df_equip = pd.concat([info, operability, er_equip, sx_equip], axis=1)

#Creat dict for entries
values_dict = {
    'Nunca ha existido'  : 5,
    'No hubo'            : 5,
    'No operativa'       : 4,
    'Entre 1 y 2 días'   : 3,
    'Menos de 3 de días' : 2,
    'Entre 3 y 5 días'   : 1,
    'Todos los días'     : 0
}

#Replace values with dict
df_equip_num = df_equip.replace(values_dict)

med_equip=df_equip_num.copy()
med_equip= med_equip.merge(hospital_names, on='hospital_code', how='left') 
med_equip['timestamp'] = med_equip['timestamp'].astype('datetime64').dt.strftime('%Y-%m-%d')

med_equip['week'] = med_equip['timestamp'].astype('datetime64').dt.week
med_equip['year'] = med_equip['timestamp'].astype('datetime64').dt.year
med_equip = med_equip.sort_values('timestamp').reset_index() #IMPORTANTE!!!

med_equip_lst = ['er_avail_defibrillator',
             'er_avail_ott_intubation',
             'er_avail_catheter',
             'er_avail_oxygen_suction',
             'sx_avail_ott_intubation',
             'sx_avail_patient_lingerie_kit',
             'sx_avail_disposables_mask_gloves_gown',
             'sx_avail_oxygen_suction'
            ]

weighted_rolling_sum(med_equip, med_equip_lst)

result_2 = operation.copy()
result_3 = med_equip.copy()

result_2 = result_2.fillna(" ")
result_3 = result_3.fillna(" ")



##############################################################################
##################### Medical Supplies ######################################

# Import Data and copy 
file = df.copy()
vene_survey = file.copy()
v_survey = vene_survey.copy()

# Fill null values with a space
v_survey = v_survey.fillna(' ')


# Change all data to lowercase
for i in v_survey.columns:
    try:
        v_survey[i] = v_survey[i].str.lower()
    except:
        pass


# Create categorical data for variables
v_survey = v_survey.replace("nunca ha existido", 4)
v_survey = v_survey.replace("nunca ha habido fórmulas lácteas", 4)

v_survey = v_survey.replace("no hay", 3)
v_survey = v_survey.replace("hay pero no funciona", 3)
v_survey = v_survey.replace("no hubo fórmulas lácteas ningún día", 3)
v_survey = v_survey.replace("no hubo", 3)

v_survey = v_survey.replace("menos de 3 de días", 2)
v_survey = v_survey.replace("menos de 3 días", 2)
v_survey = v_survey.replace("funciona menos de 3 días", 2)
v_survey = v_survey.replace("hubo formulas lácteas menos de 3 días", 2)
v_survey = v_survey.replace("entre 1 y 2 días", 2)

v_survey = v_survey.replace("entre 3 y 5 días", 1)
v_survey = v_survey.replace("funciona entre 3 y 5 días", 1)
v_survey = v_survey.replace("hubo fórmulas lácteas entre 3 y 5 días", 1)

v_survey = v_survey.replace("todos los días", 0)
v_survey = v_survey.replace("funciona todos los días", 0)
v_survey = v_survey.replace("hubo fórmulas lácteas ningún día", 0)

# Rename wahs_failure_sx to wash_failure_sx
v_survey['wash_failure_sx'] = v_survey['wahs_failure_sx']
v_survey = v_survey.drop(labels = 'wahs_failure_sx', axis = 1)


v_survey['week'] = v_survey['timestamp'].astype('datetime64').dt.week
v_survey['year'] = v_survey['timestamp'].astype('datetime64').dt.year


v_survey = v_survey.sort_values('timestamp').reset_index() #IMPORTANTE!!!


    
#Change v_survey to result

result = v_survey.copy()
    
    
# Create categorical data for variables
result = result.replace("nunca ha existido", 4)
result = result.replace("nunca ha habido fórmulas lácteas", 4)
result = result.replace("no hay", 3)
result = result.replace("hay pero no funciona", 3)
result = result.replace("no hubo fórmulas lácteas ningún día", 3)
result = result.replace("no hubo", 3)
result = result.replace("menos de 3 de días", 2)
result = result.replace("menos de 3 días", 2)
result = result.replace("funciona menos de 3 días", 2)
result = result.replace("hubo formulas lácteas menos de 3 días", 2)
result = result.replace("entre 1 y 2 días", 2)
result = result.replace("entre 3 y 5 días", 1)
result = result.replace("funciona entre 3 y 5 días", 1)
result = result.replace("hubo fórmulas lácteas entre 3 y 5 días", 1)
result = result.replace("todos los días", 0)
result = result.replace("funciona todos los días", 0)
result = result.replace("hubo fórmulas lácteas ningún día", 0)

    
## Dictionaries for categorical data
med_sup = ['er_avail_adrenalin',
           'er_avail_atropine',
           'er_avail_dopamine',
           'er_avail_cephalosporins_betalactams',
           'er_avail_aminoglycosides_quinolone',
           'er_avail_vancomycin_clindamycin',
           'er_avail_lidocaine',
           'er_avail_minor_opioids',
           'er_avail_major_opioids',
           'er_avail_iv_fluids',
           'er_avail_diazepam_dph',
           'er_avail_heparin',
           'er_avail_steroids',
           'er_avail_insulin',
           'er_avail_asthma',
           'er_avail_blood_pressure',
           'sx_avail_minor_opioids',
           'sx_avail_major_opioids',
           'sx_avail_anesthetic_gases',
           'sx_avail_anesthetics_iv',
           'sx_avail_relaxants'
          ] # plug into formula


result = result.fillna(" ")

weighted_rolling_sum(result,med_sup)

##############################################################################
##################### Send Data Frames to Google Sheets as DB ################

# =============================================================================
# Send Operations to Google Sheet
# =============================================================================

import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("cred.json", scope)

client = gspread.authorize(creds)

sheet = client.open("CodingForVenezuela").sheet1  # Open the spreadhseet

sheet.spreadsheet.values_clear(range='A1:Z1000')

sheet.spreadsheet.values_append(params={'valueInputOption': 'raw',
                                        'insertDataOption': 'OVERWRITE'}, 
                                range='Sheet1!A1',
                                body=dict(
                                        majorDimension='ROWS',
                                        values=result_2.T.reset_index().T.values.tolist()
                                        )
                                )

# =============================================================================
# Send Medical Supplies to Google Sheet
# =============================================================================
sheet2 = client.open("CodingForVenezuela_Emily").sheet1  # Open the spreadhseet

sheet2.spreadsheet.values_clear(range='A1:Z1000')

sheet2.spreadsheet.values_append(params={'valueInputOption': 'raw',
                                        'insertDataOption': 'OVERWRITE'}, 
                                range='Sheet1!A1',
                                body=dict(
                                        majorDimension='ROWS',
                                        values=result.T.reset_index().T.values.tolist()
                                        )
                                )


# =============================================================================
# Send Medical Equipment to Google Sheet
# =============================================================================
sheet3 = client.open("CodingForVenezuela_Tuba").sheet1  # Open the spreadhseet

sheet3.spreadsheet.values_clear(range='A1:Z1000')

sheet3.spreadsheet.values_append(params={'valueInputOption': 'raw',
                                        'insertDataOption': 'OVERWRITE'}, 
                                range='Sheet1!A1',
                                body=dict(
                                        majorDimension='ROWS',
                                        values=result_3.T.reset_index().T.values.tolist()
                                        )
                                )
                                

# =============================================================================
# Send Staff to Google Sheets
# =============================================================================
sheet4 = client.open("CodingForVenezuela_Staff").sheet1  # Open the spreadhseet

sheet4.spreadsheet.values_clear(range='A1:Z1000')

sheet4.spreadsheet.values_append(params={'valueInputOption': 'raw',
                                        'insertDataOption': 'OVERWRITE'}, 
                                range='Sheet1!A1',
                                body=dict(
                                        majorDimension='ROWS',
                                        values=df3.T.reset_index().T.values.tolist()
                                        )
                                )
