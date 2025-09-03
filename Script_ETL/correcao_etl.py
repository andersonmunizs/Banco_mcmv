# --- CONFIGURAÇÃO ---
nome_do_arquivo_de_entrada = 'dados_tratados.csv'

nome_do_arquivo_de_saida = 'dados_tratados_final.csv'
# --------------------

try:
    print(f"Iniciando a leitura e correção do arquivo '{nome_do_arquivo_de_entrada}'...")

    # Abre o arquivo de entrada para leitura com a codificação correta
    with open(nome_do_arquivo_de_entrada, 'r', encoding='latin-1') as arquivo_entrada:
        # Abre o arquivo de saída para escrita com a mesma codificação
        with open(nome_do_arquivo_de_saida, 'w', encoding='latin-1') as arquivo_saida:
            
            for linha in arquivo_entrada:
                # Remove espaços em branco ou quebras de linha no final
                linha_limpa = linha.strip()
                
                # garantindo que não haja vírgulas extras.
                arquivo_saida.write(f'{linha_limpa};\n')

    print("\nConcluído! O arquivo foi corrigido e salvo com sucesso.")

except FileNotFoundError:
    print(f"\nERRO: O arquivo '{nome_do_arquivo_de_entrada}' não foi encontrado.")
    print("Por favor, verifique se o nome do arquivo está correto e se ele está na mesma pasta do script.")
except Exception as e:
    print(f"\nOcorreu um erro inesperado: {e}")