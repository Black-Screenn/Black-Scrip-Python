import time
import psutil as ps
import pandas as pd
from datetime import datetime
import os
import requests
from geopy.geocoders import Nominatim
from dotenv import load_dotenv

load_dotenv()

def cadastrar():
    interfaces = ps.net_if_stats()
    Mac_address = None
    idEmpresa = input("Digite o ID da Empresa: ")

    for interface_name, interface_info in interfaces.items():
        if interface_info.isup:
            addrs = ps.net_if_addrs().get(interface_name, [])
            for addr in addrs:
                if addr.family == ps.AF_LINK:
                    Mac_address = addr.address
                    break
            if Mac_address:
                break

    ipv6 = requests.get('https://api64.ipify.org/?format=json')
    location = requests.get(f'http://ip-api.com/json/{ipv6.json().get("ip")}')
    geolocator = Nominatim(user_agent="myGeolocator")
    localizacao = geolocator.reverse(f"{location.json().get('lat')}, {location.json().get('lon')}")

    dados = {
        "idEmpresa":[idEmpresa],
        "macaddress":[Mac_address],
        "cep":[localizacao.raw.get('address').get('postcode')],
        "logradouro":[localizacao.raw.get('address').get('road')],
        "bairro":[localizacao.raw.get('address').get('suburb')],
        "cidade":[localizacao.raw.get('address').get('city')],
        "uf":[localizacao.raw.get('address').get('state')],
        "pais":[localizacao.raw.get('address').get('country')],
        "latitude":[location.json().get('lat')],
        "longitude":[location.json().get('lon')],
    }

    df = {
        "dataframe": [
            dados
        ]
    }

    Ip = os.getenv('IpAplicacao', 'http://localhost:3333')
    res = requests.post(f"http://{Ip}/caixas/cadastrar", json=df)

    print(res)

cadastrar()

    