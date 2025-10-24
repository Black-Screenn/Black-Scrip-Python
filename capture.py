import time
import psutil as ps
import pandas as pd # type: ignore
from datetime import datetime
from calendar import monthrange
import os
import requests

interfaces = ps.net_if_stats()
Mac_address = None

for interface_name, interface_info in interfaces.items():
    if interface_info.isup:
        addrs = ps.net_if_addrs().get(interface_name, [])
        for addr in addrs:
            if addr.family == ps.AF_LINK:
                Mac_address = addr.address
                break
        if Mac_address:
            break

codigoMaquina = Mac_address

def capturar(horarioCaptura, cpuUso, memUso, diskUso):
    df = None
    net = ps.net_io_counters()
    pacotes_enviados = net.packets_sent
    pacotes_recebidos = net.packets_recv
    pacotes_perdidos = net.dropin + net.dropout
   
    dados = {
        "macaddress":[],
        "datetime":[],
        "cpu":[],
        "ram":[],
        "disco":[],
        "uptime": [],
        "pacotes_enviados": [],
        "pacotes_recebidos": [],
        "pacotes_perdidos": []
    }       
    dados["macaddress"].append(codigoMaquina)
    dados["datetime"].append(horarioCaptura)
    dados["cpu"].append(cpuUso)
    dados["ram"].append(memUso)
    dados["disco"].append(diskUso)
    uptime_seconds = (datetime.now() - datetime.fromtimestamp(ps.boot_time())).total_seconds()
    dados["uptime"].append(uptime_seconds)
    dados["pacotes_enviados"].append(pacotes_enviados)
    dados["pacotes_recebidos"].append(pacotes_recebidos)
    dados["pacotes_perdidos"].append(pacotes_perdidos)

    df = pd.DataFrame(dados)
    file_name = f"capturaMaquina-{horarioCaptura.month}-{horarioCaptura.year}-{codigoMaquina}.csv"
    if(os.path.exists(file_name)):
        df.to_csv(file_name, mode="a", encoding="utf-8", index=False, sep=";", header=False)
    else:
        df.to_csv(file_name, mode="a", encoding="utf-8", index=False, sep=";")
        if os.path.exists(f"capturaMaquina-{horarioCaptura.month-1 if horarioCaptura.month-1 != 0 else 12}-{horarioCaptura.year if horarioCaptura.month-1 != 0 else horarioCaptura.year-1}-{codigoMaquina}.csv"):
            os.remove(f"capturaMaquina-{horarioCaptura.month-1 if horarioCaptura.month-1 != 0 else 12}-{horarioCaptura.year if horarioCaptura.month-1 != 0 else horarioCaptura.year-1}-{codigoMaquina}.csv")

    print(df)
    enviarS3(file_name)

def capturarProcessos(dataAtual):
    listaProcesso = {"datetime": [], "pid": [], "macaddress": [], "name": [], "status": []}
    
    for processo in ps.process_iter(['pid', 'name', 'status']):
        
        listaProcesso["datetime"].append(dataAtual)
        listaProcesso["macaddress"].append(codigoMaquina)
        listaProcesso["pid"].append(processo.pid)
        listaProcesso["name"].append(processo.info["name"])
        listaProcesso["status"].append(processo.info["status"])
        
    dfProcesso = pd.DataFrame(listaProcesso)
    file_name = f"capturaProcesso-{dataAtual.month}-{dataAtual.year}-{codigoMaquina}.csv"
    if(os.path.exists(file_name)):
        dfProcesso.to_csv(file_name, mode="a", encoding="utf-8", index=False, sep=";", header=False)
    else:
        dfProcesso.to_csv(file_name, mode="a", encoding="utf-8", index=False, sep=";")
        if os.path.exists(f"capturaProcesso-{dataAtual.month-1 if dataAtual.month-1 != 0 else 12}-{dataAtual.year if dataAtual.month-1 != 0 else dataAtual.year-1}-{codigoMaquina}.csv"):
            os.remove(f"capturaProcesso-{dataAtual.month-1 if dataAtual.month-1 != 0 else 12}-{dataAtual.year if dataAtual.month-1 != 0 else dataAtual.year-1}-{codigoMaquina}.csv")
    enviarS3(file_name)

def enviarS3(file_name):

    df = pd.read_csv(file_name, sep=";")

    data = {
        "dataframe": [
            df.to_json(orient="records")
        ]
    }

    res = requests.post(f"http://localhost:3333/cloud/enviar/{file_name}", json=data)


while True:
    datacaptura = datetime.now()
    capturar(
             datacaptura, 
             ps.cpu_percent(), ps.virtual_memory().percent, 
             ps.disk_usage('/').percent
    )
    capturarProcessos(datacaptura)
    time.sleep(60)


