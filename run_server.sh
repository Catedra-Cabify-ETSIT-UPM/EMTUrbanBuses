#!/bin/bash 
trap "kill 0" EXIT

cd Scripts/AnomaliesDetection
#python3 models_params.py
python3 detect_anoms_hws.py &

cd ../../Dashboard
python3 index.py &

wait