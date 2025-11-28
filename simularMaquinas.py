import pandas as pd
from faker import Faker
import random
import os
from datetime import datetime, timedelta
import numpy as np

fake = Faker('pt_BR')

OUTPUT_DIR = 'simulacaoDados'
NUM_MAQUINAS = 10
START_DATE = datetime(2025, 1, 1, 0, 0)
END_DATE = datetime(2025, 12, 31, 23, 59)
INTERVAL_MINUTES = 10
EMPRESA_ID = 1

def is_business_day(date):
    return date.weekday() < 5

def get_paydays(year, month):
    cal = pd.to_datetime(f'{year}-{month}-01')
    month_days = pd.date_range(cal, periods=cal.daysinmonth, freq='D')
    business_days = [d for d in month_days if is_business_day(d)]
    
    paydays = []
    if len(business_days) >= 5:
        paydays.append(business_days[4].day)
    if business_days:
        paydays.append(business_days[-1].day)
    
    return set(paydays)

def generate_and_partition_data_with_rules(output_base_dir, num_machines, start_date, end_date, interval, empresa_id):
    print(f"Iniciando a geração de dados com regras de negócio para {num_machines} máquinas...")
    
    all_records = []
    date_ranges = pd.date_range(start=start_date, end=end_date, freq=f'{interval}min')
    
    payday_cache = {}

    for i in range(num_machines):
        mac = fake.mac_address().replace(':', '')

        df_machine = pd.DataFrame({
            'macaddress': mac,
            'datetime': date_ranges,
        })
        
        df_machine['cpu'] = [random.uniform(5.0, 30.0) for _ in range(len(date_ranges))]
        df_machine['ram'] = [random.uniform(40.0, 70.0) for _ in range(len(date_ranges))]
        df_machine['disco'] = [random.uniform(10.0, 40.0) for _ in range(len(date_ranges))]
        df_machine['uptime'] = [random.uniform(100.0, 15000.0) for _ in range(len(date_ranges))]
        df_machine['bytes_enviados'] = [random.uniform(50.0, 200.0) for _ in range(len(date_ranges))]
        df_machine['bytes_recebidos'] = [random.uniform(100.0, 500.0) for _ in range(len(date_ranges))]
        df_machine['pacotes_perdidos'] = [random.randint(0, 20) for _ in range(len(date_ranges))]

        for idx, row in df_machine.iterrows():
            date = row['datetime']
            year_month = (date.year, date.month)
            if year_month not in payday_cache:
                payday_cache[year_month] = get_paydays(date.year, date.month)
            
            paydays = payday_cache[year_month]

            if date.day in paydays:
                multiplicador = random.uniform(1.5, 2.5)
                df_machine.loc[idx, 'cpu'] *= multiplicador
                df_machine.loc[idx, 'ram'] *= multiplicador
                df_machine.loc[idx, 'bytes_enviados'] *= multiplicador
                df_machine.loc[idx, 'bytes_recebidos'] *= multiplicador
                df_machine.loc[idx, 'pacotes_perdidos'] += random.randint(10, 30)

            if date.weekday() >= 5:
                multiplicador = random.uniform(1.1, 1.3)
                df_machine.loc[idx, 'cpu'] *= multiplicador
                df_machine.loc[idx, 'ram'] *= multiplicador
                df_machine.loc[idx, 'bytes_enviados'] *= multiplicador
                df_machine.loc[idx, 'bytes_recebidos'] *= multiplicador
            
            if 17 <= date.hour < 20:
                multiplicador = random.uniform(1.2, 1.8)
                df_machine.loc[idx, 'cpu'] *= multiplicador
                df_machine.loc[idx, 'ram'] *= multiplicador
                df_machine.loc[idx, 'bytes_enviados'] *= multiplicador
                df_machine.loc[idx, 'bytes_recebidos'] *= multiplicador
        
        df_machine['cpu'] = np.clip(df_machine['cpu'], 0, 100)
        df_machine['ram'] = np.clip(df_machine['ram'], 0, 100)
        
        all_records.append(df_machine)
    
    df_final = pd.concat(all_records, ignore_index=True)
    df_final = df_final.sort_values(by=['macaddress', 'datetime']).reset_index(drop=True)
    
    print(f"Geração de {len(df_final)} registros completa na memória. Iniciando particionamento...")

    df_final['ano'] = df_final['datetime'].dt.year
    df_final['mes'] = df_final['datetime'].dt.month
    df_final['dia'] = df_final['datetime'].dt.day
    df_final['datetime'] = df_final['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    for (ano, mes, dia), group in df_final.groupby(['ano', 'mes', 'dia']):
        folder_path = os.path.join(
            output_base_dir,
            f"empresa={empresa_id}",
            f"ano={ano}",
            f"mes={mes:02d}",
            f"dia={dia:02d}",
            f"tipo=maquina"
        )
        
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, 'dados.csv')
        
        group = group.drop(columns=['ano', 'mes', 'dia'])

        group.to_csv(file_path, sep=';', index=False, float_format='%.2f')
        
    print(f"Particionamento concluído. Arquivos salvos em '{output_base_dir}'.")

if __name__ == "__main__":
    generate_and_partition_data_with_rules(OUTPUT_DIR, NUM_MAQUINAS, START_DATE, END_DATE, INTERVAL_MINUTES, EMPRESA_ID)
