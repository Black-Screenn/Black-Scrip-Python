import time
import psutil as ps
import pandas as pd # type: ignore
from datetime import datetime
import os
import geopy as geo

codigoMaquina = ""

def cadastrar():
    a


def capturar(usuarioCaptura, horarioCaptura, cpuUso, memUso, diskUso):
    df = None
    net = ps.net_io_counters()
    pacotes_enviados = net.packets_sent
    pacotes_recebidos = net.packets_recv
    pacotes_perdidos = net.dropin + net.dropout
   
    dados = {
        "usuario":[],
        "timestamp":[],
        "cpu":[],
        "ram":[],
        "disco":[],
        "uptime": [],
        "pacotes_enviados": [],
        "pacotes_recebidos": [],
        "pacotes_perdidos": []
    }       
    dados["usuario"].append(usuarioCaptura)
    dados["timestamp"].append(horarioCaptura)
    dados["cpu"].append(cpuUso)
    dados["ram"].append(memUso)
    dados["disco"].append(diskUso)
    uptime_seconds = (datetime.now() - datetime.fromtimestamp(ps.boot_time())).total_seconds()
    dados["uptime"].append(uptime_seconds)
    dados["pacotes_enviados"].append(pacotes_enviados)
    dados["pacotes_recebidos"].append(pacotes_recebidos)
    dados["pacotes_perdidos"].append(pacotes_perdidos)

    df = pd.DataFrame(dados)

    if(os.path.exists('capturaMaquina.csv')):
        return df.to_csv("capturaMaquina.csv", mode="a", encoding="utf-8", index=False, sep=";", header=False)
    else:
        return df.to_csv("capturaMaquina.csv", mode="a", encoding="utf-8", index=False, sep=";")

def capturarProcessos():
    dataAtual = datetime.now()

    listaProcesso = {"datetime": [], "pid": [], "codigo": [], "name": [], "status": []}
    
    for processo in ps.process_iter(['pid', 'name', 'status']):
        
        listaProcesso["datetime"].append(dataAtual)
        listaProcesso["codigo"].append(codigoMaquina)
        listaProcesso["pid"].append(processo.pid)
        listaProcesso["name"].append(processo.info["name"])
        listaProcesso["status"].append(processo.info["status"])
        
    dfProcesso = pd.DataFrame(listaProcesso)
    if(os.path.exists('capturaProcesso.csv')):
        dfProcesso.to_csv("capturaProcesso.csv", mode="a", encoding="utf-8", index=False, sep=";", header=False)
    else:
        dfProcesso.to_csv("capturaProcesso.csv", mode="a", encoding="utf-8", index=False, sep=";")

while True:
    capturar(codigoMaquina, 
             datetime.now(), 
             ps.cpu_percent(), ps.virtual_memory().percent, 
             ps.disk_usage('/').percent
    )
    capturarProcessos()
    time.sleep(10)
