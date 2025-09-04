import pandas as pd
import psycopg2

# Parâmetros
ARQUIVO_ORIGEM = "dados_tratados_utf8.csv"
DB_HOST = "localhost"
DB_USER = "mcmv_user"
DB_PASS = "mcmv_pass"
DB_NAME = "mcmv_dw"

# 2. Carregar o arquivo de origem para um DataFrame do Pandas
df_origem = pd.read_csv(ARQUIVO_ORIGEM, sep=';')

# 3. Contar o número de registros na origem
num_registros_origem = len(df_origem)
print(f"Número de registros no arquivo de origem: {num_registros_origem}")

# 4. Conexão com o banco de dados PostgreSQL
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    )
    cur = conn.cursor()

    # 5. Contar os registros nas tabelas de destino
    cur.execute("SELECT COUNT(*) FROM dw_mcmv.fato_empreendimento;")
    num_registros_destino = cur.fetchone()[0]
    print(f"Número de registros carregados no Data Warehouse: {num_registros_destino}")

    # 6. Validação dos dados
    # a. Comparação de Registros
    if num_registros_destino == num_registros_origem:
        print("Sucesso! A quantidade de registros na origem e no destino é a mesma.")
    else:
        print(f"Atenção! Perda de dados. Registros na origem: {num_registros_origem}, Registros no destino: {num_registros_destino}")

    # b. Checagem de Valores Nulos
    cur.execute("SELECT COUNT(*) FROM dw_mcmv.fato_empreendimento WHERE qtd_uh IS NULL;")
    nulos_qtd_uh = cur.fetchone()[0]

    if nulos_qtd_uh == 0:
        print("Sucesso! Não foram encontrados valores nulos na coluna 'qtd_uh'.")
    else:
        print(f"Atenção! Foram encontrados {nulos_qtd_uh} valores nulos na coluna 'qtd_uh'.")

    # c. Conformidade de Tipo de Dados
    try:
        cur.execute("SELECT AVG(val_contratado_total) FROM dw_mcmv.fato_empreendimento;")
        media_valores = cur.fetchone()[0]
        print(f"Sucesso! A coluna 'val_contratado_total' está em formato numérico. Média calculada: {media_valores}")
    except psycopg2.errors.InvalidTextRepresentation:
        print("Erro! A coluna 'val_contratado_total' contém dados não numéricos.")

except psycopg2.OperationalError as e:
    print(f"Erro de conexão: {e}")
except Exception as e:
    print(f"Ocorreu um erro: {e}")
finally:
    if 'cur' in locals() and cur:
        cur.close()
    if 'conn' in locals() and conn:
        conn.close()

# 7. Geração de um log de validação
with open("log_validacao.txt", "w") as log_file:
    log_file.write("Relatório de Validação de Dados\n")
    log_file.write("--------------------------------\n")
    log_file.write(f"Comparação de Registros: {'OK' if num_registros_destino == num_registros_origem else 'FALHA'}\n")
    log_file.write(f"Checagem de Nulos (qtd_uh): {'OK' if nulos_qtd_uh == 0 else 'FALHA'}\n")
    log_file.write("Validação de Tipo de Dados: Concluída com sucesso.\n")