import time
import psutil as ps
import pandas as pd
from datetime import datetime
import os
import random

qtd_maquinas = 6

def gerar_metricas_simuladas(machine_id):
    # Linha base para a simulação de dados
    cpu_base = random.uniform(10.0, 30.0)
    ram_base = random.uniform(50.0, 70.0)
    disk_base = random.uniform(30.0, 70.0)

    if 'machine_3' in machine_id: 
        cpu_base = random.uniform(30.0, 80.0)
    elif 'machine_5' in machine_id: 
        ram_base = random.uniform(65.0, 93.0)

    cpu_uso = round(min(100.0, cpu_base + random.uniform(-5.0, 5.0)), 2)
    mem_uso = round(min(100.0, ram_base + random.uniform(-5.0, 5.0)), 2)
    disk_uso = round(min(100.0, disk_base + random.uniform(-5.0, 5.0)), 2)

    bytes_enviados = random.randint(100000, 5000000)
    bytes_recebidos = random.randint(500000, 10000000)
    pacotes_perdidos = random.randint(0, 10) 
    uptime_seconds = random.randint(86400 * 5, 86400 * 30) # Entre 5 e 30 dias

    return cpu_uso, mem_uso, disk_uso, uptime_seconds, bytes_enviados, bytes_recebidos, pacotes_perdidos

def capturar(horarioCaptura, maquinaID, cpuUso, memUso, diskUso, uptime, bytes_enviados, bytes_recebidos, pacotes_perdidos):
    
    dados = {
        "codigo_maquina": [maquinaID],
        "timestamp": [horarioCaptura],
        "cpu": [cpuUso],
        "ram": [memUso],
        "disco": [diskUso],
        "uptime": [uptime],
        "bytes_enviados": [bytes_enviados],
        "bytes_recebidos": [bytes_recebidos],
        "pacotes_perdidos": [pacotes_perdidos]
    }
            
    df = pd.DataFrame(dados)
    
    if(os.path.exists('massaMaquina.csv')):
        return df.to_csv("massaMaquina.csv", mode="a", encoding="utf-8", index=False, sep=";", header=False)
    else:
        return df.to_csv("massaMaquina.csv", mode="a", encoding="utf-8", index=False, sep=";")


def capturarProcessos(maquinaID, dataAtual):
    if maquinaID != 'machine_1':
        return 

    listaProcesso = {"codigo_maquina": [], "datetime": [], "pid": [], "name": [], "status": []} # Removida a chave "codigo"
    
    for processo in ps.process_iter(['pid', 'name', 'status']):
        listaProcesso["codigo_maquina"].append(maquinaID) 
        listaProcesso["datetime"].append(dataAtual)
        listaProcesso["pid"].append(processo.pid)
        listaProcesso["name"].append(processo.info["name"])
        listaProcesso["status"].append(processo.info["status"])
        
    dfProcesso = pd.DataFrame(listaProcesso)

    if(os.path.exists('massaProcesso.csv')):
        dfProcesso.to_csv("massaProcesso.csv", mode="a", encoding="utf-8", index=False, sep=";", header=False)
    else:
        dfProcesso.to_csv("massaProcesso.csv", mode="a", encoding="utf-8", index=False, sep=";")


# Início do loop para gerar os dados de cada máquina
while True:
    horario_captura = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for i in range(1, qtd_maquinas + 1):
        codigo_maquina = f'machine_{i}'

        if i == 1:
            cpu_uso = ps.cpu_percent(interval=None) 
            mem_uso = ps.virtual_memory().percent
            disk_uso = ps.disk_usage('/').percent
            net = ps.net_io_counters()
            bytes_enviados = net.bytes_sent
            bytes_recebidos = net.bytes_recv
            pacotes_perdidos = net.dropin + net.dropout
            uptime_seconds = (datetime.now() - datetime.fromtimestamp(ps.boot_time())).total_seconds()
            
            capturarProcessos(codigo_maquina, horario_captura)
        else:
            (cpu_uso, mem_uso, disk_uso, uptime_seconds, bytes_enviados, bytes_recebidos, pacotes_perdidos) = gerar_metricas_simuladas(codigo_maquina)
            
        capturar(
            horario_captura, codigo_maquina, cpu_uso, mem_uso, disk_uso, 
            uptime_seconds, bytes_enviados, bytes_recebidos, pacotes_perdidos
        )
        
    print(f"[{horario_captura}] Dados de {qtd_maquinas} máquinas registrados.")
    time.sleep(10)