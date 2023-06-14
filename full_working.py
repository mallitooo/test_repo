# -*- coding: utf-8 -*-
"""
Created on Mon May  8 11:19:25 2023

@author: a898672
"""
import paho.mqtt.client as mqtt 
import ast
import time 
from pandas import DataFrame
from scipy.ndimage import gaussian_filter
import os

import import_utils as f

global predictions_df 
predictions_df = DataFrame(columns= ['Mach', 'TRA', 'T2', 'T24', 'T30', 'T48', 'T50', 'P15', 'P2', 'P21','P24', 'Ps30', 'P40', 'P50', 'Nf', 'Nc', 'Wf','raw_RUL', 'filt_RUL', 'raw_HS', 'filt_HS'])

regressor = f.import_working_regressor(r'/home/keras_models/deployment_mini_regressor')
classifier = f.import_working_classifier(r'/home/keras_models/deployment_mini_classifier')

client = mqtt.Client(client_id = 'container_listener')

def on_conc(client, userdata, flags, rc):
    if rc == 0:
        print(f"Conexión establecida con éxito al broker: {client._host}.")
    else:
        print("Error al conectar con el broker MQTT. Código de retorno: ", rc)

client.on_connect= on_conc

def on_msg(client, userdata, msg):
    start_time = time.time()
    i, values = ast.literal_eval(msg.payload.decode("utf-8"))
    
    global predictions_df
    
    if i not in predictions_df.index:
        predictions_df.loc[i]= [None]*len(predictions_df.columns)
        
    if msg.topic == 'flight_envelope':
        predictions_df.iloc[i, :3]= values    
    
    if msg.topic == 'temperatures':
        predictions_df.iloc[i, 3:7]= values    
    
    if msg.topic == 'pressures':
        predictions_df.iloc[i, 7:14]= values  
        
    if msg.topic == 'axis_control':
        predictions_df.iloc[i, 14:-4]= values  
    
    if not predictions_df.iloc[i, :-4].isna().any(): 
        prediction_RUL = regressor.predict(DataFrame([predictions_df.iloc[i,:-4]], columns = predictions_df.columns[:-4]), verbose = 0)
        #prediction_RUL = [[555]]
        prediction_HS = classifier.predict(DataFrame([predictions_df.iloc[i,:-4]], columns = predictions_df.columns[:-4]), verbose = 0)
        #prediction_HS = [[1]]
        predictions_df.iloc[i,-4] = round(prediction_RUL[0][0]/3600, 3)
        predictions_df.iloc[i,-3] = round(gaussian_filter([elem for elem in predictions_df.raw_RUL], sigma = 700)[-1], 3)
        predictions_df.iloc[i,-2] = round(prediction_HS[0][0], 3)
        predictions_df.iloc[i,-1] = round(gaussian_filter([elem for elem in predictions_df.raw_HS], sigma = 700)[-1])
        print(f'\n[MESSAGE]\tPred.RUL:{predictions_df.iloc[i,-4]} || Filt.RUL:{predictions_df.iloc[i,-3]}h. || Pred.HS:{predictions_df.iloc[i,-2]:.3f} || Filt.HS:{predictions_df.iloc[i,-1]} || Time:{round(time.time()-start_time, 3)}s')
    
client.on_message= on_msg

broker_host = os.environ.get('BROKER_HOST')

client.connect(host=broker_host , port= 1883)

client.subscribe(topic= 'flight_envelope')
client.subscribe(topic= 'temperatures')
client.subscribe(topic= 'pressures')
client.subscribe(topic= 'axis_control')

print('\n[LISTENING]\t Client listening: \n')
client.loop_forever()