import time
import psutil as ps
import pandas as pd # type: ignore
from datetime import datetime
import os
import geopy
import requests
from geopy.geocoders import Nominatim

def obter_localizacao():

    response = requests.get('http://ipinfo.io/json', timeout=10)
    data = response.json()
    
    if 'loc' in data:
        lat, lon = data['loc'].split(',')
        return float(lat), float(lon)
        
    geolocator = Nominatim(user_agent="cadastro_app")
    location = geolocator.geocode("Brazil")  
 
    return location.latitude, location.longitude


def cadastrar():
    interfaces = ps.net_if_addrs()
    Mac_address = None
    
    Latitude, Longitude = obter_localizacao()
    
    print("lat: " , Latitude,
          "lon: " , Longitude)

    for nome_interface, enderecos in interfaces.items():
        for endereco in enderecos:
            if endereco.family == ps.AF_LINK:
                Mac_address = endereco.address

    dados = {
        "macaddress":[Mac_address],
        "latitude":[Latitude],
        "longitude":[Longitude]
    } 

    df = pd.DataFrame(dados)

    if(os.path.exists('cadastro.csv')):
        return df.to_csv("cadastro.csv", mode="a", encoding="utf-8", index=False, sep=";", header=False)
    else:
        return df.to_csv("cadastro.csv", mode="a", encoding="utf-8", index=False, sep=";")
    
cadastrar()

    