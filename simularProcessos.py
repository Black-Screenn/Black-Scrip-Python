import pandas as pd
import random
import os
import copy  # Importante para corrigir o bug de alteração de status

DATA_INICIO = '2025-01-01 00:00:00'
DATA_FIM    = '2025-12-03 23:50:00'
INTERVALO_MINUTOS = 10

mac_addresses = [
    '02bbbdc02bf9', '16c3ad24476b', '1817c27bfb2d', '28b582796f26',
    '56d446c142d2', '963c48bb0adf', 'bc7673e2bc48', 'c8a006971278',
    'd8408e1114d1', 'f0185cdf891b'
]

servicos_config = {
    'interface_grafica': 7,
    'impressora': 1,
    'dispenser_dinheiro': 1,
    'gerenciador_rede': 1,
    'manipulador_transacoes': 2,
    'leitor_digitais': 1,
    'leitor_cartoes': 1
}

def gerar_estado_inicial(mac_addrs, config):
    estado_maquinas = {}
    for mac in mac_addrs:
        processos = []
        for servico, qtd_base in config.items():
            qtd_real = qtd_base + random.randint(0, 1)
            
            random_value = random.randint(1, 10) 
            status = "running" if random_value < 9 else "sleeping"

            for _ in range(qtd_real):
                processos.append({
                    'pid': random.randint(100, 65000),
                    'atm_service': servico,
                    'usuario': 'dandansousa',
                    'status': status # Status base
                })
        estado_maquinas[mac] = processos
    return estado_maquinas

estado_fixo = gerar_estado_inicial(mac_addresses, servicos_config)

dias_range = pd.date_range(start=DATA_INICIO, end=DATA_FIM, freq='D')

print(f"Iniciando simulação dia a dia ({len(dias_range)} dias)...")

total_arquivos = 0

for dia_corrente in dias_range:
    fim_do_dia = dia_corrente + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    limite_loop = min(fim_do_dia, pd.to_datetime(DATA_FIM))
    
    intervalos_dia = pd.date_range(start=dia_corrente, end=limite_loop, freq=f'{INTERVALO_MINUTOS}T')
    
    dados_do_dia = []

    data_inicio_sleep = pd.to_datetime("2025-11-23")
    data_fim_sleep = pd.to_datetime("2025-11-30")
    for timestamp in intervalos_dia:
        str_data = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        eh_periodo_sleep = (timestamp >= data_inicio_sleep and timestamp <= data_fim_sleep)

        for mac, processos in estado_fixo.items():
            for proc in processos:
                status_atual = proc['status']
                
                if eh_periodo_sleep and mac == "56d446c142d2":
                    status_atual = "sleeping"

                dados_do_dia.append({
                    'datetime': str_data, # Usamos string direto para o CSV
                    'pid': proc['pid'],
                    'macaddress': mac,
                    'atm_service': proc['atm_service'],
                    'status': status_atual,
                    'usuario': proc['usuario']
                })

    # --- Fim do processamento do dia, hora de salvar ---
    
    if not dados_do_dia:
        continue

    df_dia = pd.DataFrame(dados_do_dia)
    
    # Extrair ano, mes, dia do objeto timestamp atual (dia_corrente)
    str_ano = str(dia_corrente.year)
    str_mes = f"{dia_corrente.month:02d}"
    str_dia = f"{dia_corrente.day:02d}"

    # Como estamos no loop do dia, já podemos agrupar só por MAC
    grupos_mac = df_dia.groupby('macaddress')

    for mac, grupo in grupos_mac:
        caminho_pasta = os.path.join(
            "simularProcessos",
            "empresa=1",
            f"ano={str_ano}",
            f"mes={str_mes}",
            f"dia={str_dia}",
            "tipo=processo"
        )
        
        os.makedirs(caminho_pasta, exist_ok=True)
        nome_arquivo = f"{mac}.csv"
        caminho_completo = os.path.join(caminho_pasta, nome_arquivo)
        
        grupo.to_csv(caminho_completo, index=False, sep=';')
        total_arquivos += 1
    
    print(f"Dia {str_dia}/{str_mes}/{str_ano} processado e salvo.")

print(f"\nConcluído! Total de arquivos gerados: {total_arquivos}")