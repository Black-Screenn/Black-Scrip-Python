import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

# ================= CONFIGURAÇÕES GERAIS =================
OUTPUT_DIR = 'simularMaquinasClientDan'
EMPRESA_ID = 1

# Datas
DATA_INICIO = datetime(2025, 1, 1, 0, 0)
DATA_FIM    = datetime(2025, 12, 3, 23, 50) 
INTERVALO_MINUTOS = 10

mac_addresses = [
    '02bbbdc02bf9', '16c3ad24476b', '1817c27bfb2d', '28b582796f26',
    '56d446c142d2', '963c48bb0adf', 'bc7673e2bc48', 'c8a006971278',
    'd8408e1114d1', 'f0185cdf891b'
]

def is_business_day(date):
    return date.weekday() < 5

def get_paydays(year, month):
    """Retorna o 5º dia útil e o último dia útil do mês"""
    cal = pd.to_datetime(f'{year}-{month}-01')
    month_days = pd.date_range(cal, periods=cal.daysinmonth, freq='D')
    business_days = [d for d in month_days if is_business_day(d)]
    
    paydays = set()
    if len(business_days) >= 5:
        paydays.add(business_days[4].day) # 5º dia útil
    if business_days:
        paydays.add(business_days[-1].day) # Último dia útil
    
    return paydays

def gerar_dados_computador_unificado():
    print(f"Iniciando simulação UNIFICADA de {DATA_INICIO} até {DATA_FIM}...")
    
    payday_cache = {}

    dias_para_processar = pd.date_range(start=DATA_INICIO, end=DATA_FIM, freq='D')

    for dia_corrente in dias_para_processar:
        start_dia = dia_corrente
        end_dia = min(dia_corrente + timedelta(hours=23, minutes=50), DATA_FIM)
        
        if start_dia > DATA_FIM:
            break

        print(f"Processando dia: {start_dia.strftime('%Y-%m-%d')}")
        
        time_range = pd.date_range(start=start_dia, end=end_dia, freq=f'{INTERVALO_MINUTOS}T')
        num_registros = len(time_range)
        
        if num_registros == 0:
            continue

        ano, mes, dia_num = start_dia.year, start_dia.month, start_dia.day
        key_payday = (ano, mes)
        
        if key_payday not in payday_cache:
            payday_cache[key_payday] = get_paydays(ano, mes)
            
        eh_pagamento = dia_num in payday_cache[key_payday]
        eh_fim_semana = start_dia.weekday() >= 5
        
        todos_dados_dia = []

        for mac in mac_addresses:
            df = pd.DataFrame({'datetime': time_range})
            df['macaddress'] = mac
                        
            df['cpu'] = np.random.uniform(5.0, 30.0, num_registros)
            df['ram'] = np.random.uniform(40.0, 70.0, num_registros)
            df['disco'] = np.random.uniform(10.0, 40.0, num_registros)
            df['uptime'] = np.random.uniform(1000.0, 15000.0, num_registros) 
            
            df['bytes_enviados'] = np.random.uniform(50.0, 200.0, num_registros)
            df['bytes_recebidos'] = np.random.uniform(100.0, 500.0, num_registros)
            df['pacotes_perdidos'] = np.random.randint(0, 5, num_registros)

            horas = df['datetime'].dt.hour
            
            fator_cpu = np.ones(num_registros)
            fator_rede = np.ones(num_registros)
            
            if eh_pagamento:
                df['pacotes_perdidos'] += np.random.randint(10, 30, num_registros)
                fator_cpu *= np.random.uniform(1.5, 2.5, num_registros)
                fator_rede *= np.random.uniform(1.5, 2.5, num_registros)

            if eh_fim_semana:
                fator_cpu *= np.random.uniform(1.1, 1.3, num_registros)
                fator_rede *= np.random.uniform(1.1, 1.3, num_registros)
            
            mask_pico = ((horas >= 11) & (horas < 14)) | ((horas >= 17) & (horas < 20))
            
            if mask_pico.any():
                qtd_pico = mask_pico.sum()
                fator_cpu[mask_pico] *= np.random.uniform(1.3, 1.9, qtd_pico)
                fator_rede[mask_pico] *= np.random.uniform(1.3, 1.9, qtd_pico)

            if mac == 'f0185cdf891b':
                df['disco'] = np.random.uniform(90.0, 99.0, num_registros)
                df['pacotes_perdidos'] += np.random.randint(20, 50, num_registros)

            df['cpu'] *= fator_cpu
            df['ram'] *= fator_cpu 
            df['bytes_enviados'] *= fator_rede
            df['bytes_recebidos'] *= fator_rede

            df['cpu'] = df['cpu'].clip(0, 100).round(2)
            df['ram'] = df['ram'].clip(0, 100).round(2)
            df['disco'] = df['disco'].clip(0, 100).round(2)
            
            todos_dados_dia.append(df)

        if not todos_dados_dia:
            continue
            
        df_dia_final = pd.concat(todos_dados_dia, ignore_index=True)
        
        # Ordena por Data e depois por MAC para o CSV ficar organizado
        df_dia_final = df_dia_final.sort_values(by=['datetime', 'macaddress'])
        
        str_ano = str(ano)
        str_mes = f"{mes:02d}"
        str_dia = f"{dia_num:02d}"
        
        pasta_base = os.path.join(
            OUTPUT_DIR,
            f"empresa={EMPRESA_ID}",
            f"ano={str_ano}",
            f"mes={str_mes}",
            f"dia={str_dia}",
            "tipo=computador" 
        )
        
        os.makedirs(pasta_base, exist_ok=True)
        
        df_dia_final['datetime'] = df_dia_final['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        caminho_arquivo = os.path.join(pasta_base, "dados.csv")
        
        df_dia_final.to_csv(caminho_arquivo, sep=';', index=False)

    print(f"Simulação concluída! Dados salvos em '{OUTPUT_DIR}'")

if __name__ == "__main__":
    gerar_dados_computador_unificado()