import os
import time
from datetime import datetime
from uuid import getnode as get_macaddress

import pandas as pd
import psutil as ps
import requests
from dotenv import load_dotenv

load_dotenv()


codigoMaquina = get_macaddress()
usuario = os.getlogin()


def get_public_ip_and_isp():
    try:
        response = requests.get("https://ipinfo.io/json", timeout=5)
        if response.status_code == 200:
            data = response.json()
            ip_publico = data.get("ip")
            isp = data.get("org")
            return ip_publico, isp
        else:
            return None, None
    except Exception as e:
        print(f"Erro ao obter IP público e ISP: {e}")
        return None, None


def capturar(horarioCaptura, cpuUso, memUso, diskUso):
    ip_publico, isp = get_public_ip_and_isp()
    net = ps.net_io_counters()
    bytes_enviados = net.bytes_sent / (1024**2)
    bytes_recebidos = net.bytes_recv / (1024**2)
    pacotes_perdidos = net.dropin + net.dropout

    dados = {
        "macaddress": [],
        "datetime": [],
        "cpu": [],
        "ram": [],
        "disco": [],
        "uptime": [],
        "bytes_enviados": [],
        "bytes_recebidos": [],
        "pacotes_perdidos": [],
        "usuario": [],
        "ip_publico": [],
        "isp": [],
    }
    dados["macaddress"].append(codigoMaquina)
    dados["datetime"].append(horarioCaptura)
    dados["cpu"].append(cpuUso)
    dados["ram"].append(memUso)
    dados["disco"].append(diskUso)
    uptime_seconds = (
        datetime.now() - datetime.fromtimestamp(ps.boot_time())
    ).total_seconds()
    dados["uptime"].append(uptime_seconds)
    dados["bytes_enviados"].append(bytes_enviados)
    dados["bytes_recebidos"].append(bytes_recebidos)
    dados["pacotes_perdidos"].append(pacotes_perdidos)
    dados["usuario"].append(usuario)
    dados["ip_publico"].append(ip_publico)
    dados["isp"].append(isp)

    df = pd.DataFrame(dados)
    file_name = f"capturaMaquina-{horarioCaptura.month}-{horarioCaptura.year}-{codigoMaquina}.csv"
    if os.path.exists(file_name):
        df.to_csv(
            file_name, mode="a", encoding="utf-8", index=False, sep=";", header=False
        )
    else:
        df.to_csv(file_name, mode="a", encoding="utf-8", index=False, sep=";")
        prev_month = horarioCaptura.month - 1 if horarioCaptura.month > 1 else 12
        prev_year = (
            horarioCaptura.year if horarioCaptura.month > 1 else horarioCaptura.year - 1
        )
        prev_file = f"capturaMaquina-{prev_month}-{prev_year}-{codigoMaquina}.csv"
        if os.path.exists(prev_file):
            os.remove(prev_file)
    print(df)
    enviarS3(file_name)


def capturarProcessos(dataAtual):
    listaProcesso = {
        "datetime": [],
        "pid": [],
        "macaddress": [],
        "atm_service": [],
        "status": [],
        "usuario": [],
    }

    atm_services = {
        "chrome": "manipulador_transacoes",
        "code": "gerenciador_rede",
        "python3": "dispenser_dinheiro",
        "systemd": "interface_grafica",
        "rstudio": "leitor_cartao",
        "bash":"leitor_digitais",
        "jetbrains-toolbox":"impressora"
    }

    processos_adicionados = []
    for processo in ps.process_iter(['pid','name', 'status']):
        nome_real = processo.info["name"].lower()
        for real, atm_service in atm_services.items():
            if real in nome_real and nome_real not in processos_adicionados:
                processos_adicionados.append(nome_real)
                listaProcesso["datetime"].append(dataAtual)
                listaProcesso["macaddress"].append(codigoMaquina)
                listaProcesso["pid"].append(processo.pid)
                listaProcesso["atm_service"].append(atm_service)
                listaProcesso["status"].append(processo.info.get("status"))
                listaProcesso["usuario"].append(usuario)

    dfProcesso = pd.DataFrame(listaProcesso)
    file_name = (
        f"capturaProcesso-{dataAtual.month}-{dataAtual.year}-{codigoMaquina}.csv"
    )
    if os.path.exists(file_name):
        dfProcesso.to_csv(
            file_name, mode="a", encoding="utf-8", index=False, sep=";", header=False
        )
    else:
        dfProcesso.to_csv(file_name, mode="a", encoding="utf-8", index=False, sep=";")
        prev_month = dataAtual.month - 1 if dataAtual.month > 1 else 12
        prev_year = dataAtual.year if dataAtual.month > 1 else dataAtual.year - 1
        prev_file = f"capturaProcesso-{prev_month}-{prev_year}-{codigoMaquina}.csv"
        if os.path.exists(prev_file):
            os.remove(prev_file)
    enviarS3(file_name)


def enviarS3(file_name):
    df = pd.read_csv(file_name, sep=";")
    data = {"dataframe": [df.to_json(orient="records")]}
    Ip = os.getenv("IpAplicacao", "localhost:3333")
    res = requests.post(f"http://{Ip}/cloud/enviar/{file_name}", json=data)


while True:
    datacaptura = datetime.now().replace(microsecond=0)
    capturar(
        datacaptura,
        ps.cpu_percent(),
        ps.virtual_memory().percent,
        ps.disk_usage("/").percent,
    )
    capturarProcessos(datacaptura)
    time.sleep(60)
