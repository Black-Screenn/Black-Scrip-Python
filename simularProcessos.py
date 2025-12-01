import pandas as pd
import random
import os

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
            for _ in range(qtd_real):
                processos.append({
                    'pid': random.randint(100, 65000),
                    'atm_service': servico,
                    'usuario': 'dandansousa',
                    'status': 'sleeping'
                })
        estado_maquinas[mac] = processos
    return estado_maquinas

print("1. Gerando dados de Processos em memória...")

estado_fixo = gerar_estado_inicial(mac_addresses, servicos_config)
datas = pd.date_range(start=DATA_INICIO, end=DATA_FIM, freq=f'{INTERVALO_MINUTOS}T')
dados_simulados = []

for timestamp in datas:
    str_data = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    dt_objeto = timestamp 
    
    for mac, processos in estado_fixo.items():
        for proc in processos:
            dados_simulados.append({
                'datetime': str_data,
                'datetime_obj': dt_objeto, # Auxiliar
                'pid': proc['pid'],
                'macaddress': mac,
                'atm_service': proc['atm_service'],
                'status': proc['status'],
                'usuario': proc['usuario']
            })

df = pd.DataFrame(dados_simulados)

print(f"2. Salvando arquivos (Total de registros: {len(df)})...")

grupos = df.groupby([
    df['datetime_obj'].dt.year, 
    df['datetime_obj'].dt.month, 
    df['datetime_obj'].dt.day,
    df['macaddress']
])

contador_arquivos = 0

for (ano, mes, dia, mac), grupo in grupos:
    str_ano = str(ano)
    str_mes = f"{mes:02d}"
    str_dia = f"{dia:02d}"
    
    # 1. Cria o caminho da pasta (SEM o macaddress aqui)
    caminho_pasta = os.path.join(
        "simularProcessos",
        f"empresa=1",
        f"ano={str_ano}",
        f"mes={str_mes}",
        f"dia={str_dia}",
        "tipo=processo"
    )
    
    # Garante que a pasta existe
    os.makedirs(caminho_pasta, exist_ok=True)
    
    # 2. Define o nome do arquivo com o MAC
    nome_arquivo = f"{mac}.csv"
    caminho_completo = os.path.join(caminho_pasta, nome_arquivo)
    
    # 3. Limpa coluna auxiliar e salva
    df_final = grupo.drop(columns=['datetime_obj'])
    df_final.to_csv(caminho_completo, index=False, sep=';')
    
    contador_arquivos += 1

print(f"\nConcluído! {contador_arquivos} arquivos criados.")
print(f"Exemplo de saída: {caminho_completo}")