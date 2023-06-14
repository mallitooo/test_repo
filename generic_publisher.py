# -*- coding: utf-8 -*-
"""
Created on Fri May  5 10:50:15 2023


Build a clear and concise documentation of the following code: 


@author: a898672
"""

import time

import paho.mqtt.client as mqtt 
import os

import ncmapss_poc_utils.import_utils  as myfuns

print('\n------------------------------------------------------------')
print('\n---                GENERAL PUBLISHER                     ---')
print('\n------------------------------------------------------------')

df, labels = myfuns.import_unit_cycle_file(2, 1)

client = mqtt.Client(client_id = 'publisher')

def on_conc(client, userdata, flags, rc):
    if rc == 0:
        print(f"Conexión establecida con éxito al broker: {client._host}.")
    else:
        print("Error al conectar con el broker MQTT. Código de retorno: ", rc)

client.on_connect= on_conc

BROKER_HOST = os.environ.get('BROKER_HOST')

client.connect(host=BROKER_HOST, port=1883)

i= 0
start_time = time.time()

print('\n[PUBLISHING]\t Carrying out publishing procedure...')

intervals = 90

while time.time()- start_time < intervals:
    print('Published observation') 
    full_row = df.iloc[round((df.shape[0]-1)/intervals) *i, :].tolist()
    flight_envelope = full_row[:3]
    temperatures = full_row[3:7]
    pressures = full_row[7:14]
    axis_control = full_row[14:]
        
    client.publish(topic= 'flight_envelope', payload= str((i, [round(elem, 3) for elem in flight_envelope])))
    client.publish(topic= 'temperatures', payload= str((i, [round(elem, 3) for elem in temperatures])))
    client.publish(topic= 'pressures', payload= str((i, [round(elem, 3) for elem in pressures])))
    client.publish(topic= 'axis_control', payload= str((i, [round(elem, 3) for elem in axis_control])))
    time.sleep(1)
    i += 1 
print('\n[PUBLISHING]\t Publishing procedure has finished')
    
    



   