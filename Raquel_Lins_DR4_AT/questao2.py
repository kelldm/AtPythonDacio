import pandas as pd
from datetime import datetime

def ler_arquivos():
    """
    Lê arquivos CSV, JSON e Excel contendo dados de usuários.

    Tenta ler os arquivos 'usuarios.csv', 'usuarios.json' e 'usuarios.xlsx' na pasta 'Usuarios'.
    Se algum arquivo não for encontrado, um DataFrame vazio é atribuído à variável correspondente.

    Returns:
    - dataframe_csv (pandas DataFrame): DataFrame contendo dados lidos do arquivo CSV.
    - dataframe_json (pandas DataFrame): DataFrame contendo dados lidos do arquivo JSON.
    - dataframe_excel (pandas DataFrame): DataFrame contendo dados lidos do arquivo Excel.
    """
    dataframe_csv = pd.DataFrame()
    dataframe_json = pd.DataFrame()
    dataframe_excel = pd.DataFrame()

    try:
        dataframe_csv = pd.read_csv('Usuarios/usuarios.csv')
    except FileNotFoundError:
        print(">>ERROR:  'usuarios.csv' NÃO ENCONTRADO<<")
    except Exception as e:
        print(f">>ERROR: ERRO NA LEITURA DE ARQUIVO CSV<< {str(e)}")

    try:
        dataframe_json = pd.read_json('Usuarios/usuarios.json')
    except FileNotFoundError:
        print(">>ERROR: 'usuarios.json'NÃO ENCONTRADO<<")
    except Exception as e:
        print(f">>ERROR: ERRO NA LEITURA DE ARQUIVO JSON<< {str(e)}")

    try:
        dataframe_excel = pd.read_excel('Usuarios/usuarios.xlsx')
    except FileNotFoundError:
        print(">>ERROR: 'usuarios.xlsx' NÃO ENCONTRADO<<")
    except Exception as e:
        print(f">>ERROR: ERRO NA LEITURA DE ARQUVO XLSX<< {str(e)}")

    return dataframe_csv, dataframe_json, dataframe_excel

def limpar_dados(dataframe):
    """
    Limpa e padroniza a coluna 'data_nascimento' do DataFrame.

    Aplica a função data_nova para padronizar e validar datas,
    converte para objetos datetime e formata para o formato 'YYYY/MM/DD'.
    Remove linhas onde a data não pôde ser padronizada.

    Args:
    - dataframe (pandas DataFrame): DataFrame contendo a coluna 'data_nascimento' a ser limpa.

    Returns:
    - dataframe_limpo (pandas DataFrame): DataFrame com a coluna 'data_nascimento' limpa e padronizada.
    """
    try:
        dataframe = dataframe.astype(str)
        # Aplica a função data_nova em todas as datas da coluna 'data_nascimento'
        dataframe['data_nascimento'] = dataframe['data_nascimento'].apply(data_nova)
        
        # Converte para datetime do pandas, com erro 'coerce' para tratamento de datas inválidas
        dataframe['data_nascimento'] = pd.to_datetime(dataframe['data_nascimento'], errors='coerce')
        
        # Formata a data para 'YYYY/MM/DD'
        dataframe['data_nascimento'] = dataframe['data_nascimento'].dt.strftime('%Y/%m/%d')

        # Remove linhas sem dados.
        dataframe = dataframe.dropna()

        # Separa valores de jogos_preferidos e consoles por vírgula
        dataframe['jogos_preferidos'] = dataframe['jogos_preferidos'].str.split('|')
        dataframe['consoles'] = dataframe['consoles'].str.split('|')

        return dataframe
    
    except Exception as e:
        print(f">>ERROR<< {str(e)}")
        return pd.DataFrame()

#OBRIGADO ESTAGIARIO! 
def data_nova(data):
    try:
        if len(data) == 8 and '/' in data:
            partes = data.split('/')
            ano = partes[0]
            if int(ano) > 20:
                ano = '19' + ano
            else:
                ano = '20' + ano
            #data_formatada é uma string
            data_formatada = f"{ano}-{partes[1]}-{partes[2]}"
            
            #strptime passa data_formatada(String) para o tipo objeto datetime
            #datas como 30 de fevereiro vão dar erro, pois não existem
            datetime.strptime(data_formatada, '%Y-%m-%d')
            return data_formatada
        else:
            return data
    except (ValueError, IndexError):
        return None
    
def unificar_dados(df_csv, df_json, df_excel):
    """
    Unifica os DataFrames df_csv, df_json e df_excel em um único DataFrame.
    Limpa a coluna 'data_nascimento' e adiciona uma nova coluna 'id' única para cada usuário.

    Args:
    - df_csv (pandas DataFrame): DataFrame contendo dados lidos do arquivo CSV.
    - df_json (pandas DataFrame): DataFrame contendo dados lidos do arquivo JSON.
    - df_excel (pandas DataFrame): DataFrame contendo dados lidos do arquivo Excel.

    Returns:
    - dataframe_unificado_limpo (pandas DataFrame): DataFrame unificado e limpo com colunas reorganizadas.
    """
    try:
        # Concatenar os DataFrames em um único DataFrame
        dataframe_unificado = pd.concat([df_csv, df_json, df_excel], ignore_index=True)
        
        # Remover a coluna 'id' se existir
        if 'id' in dataframe_unificado.columns:
            dataframe_unificado.drop(columns=['id'], inplace=True)
        
        # Limpar e padronizar a coluna 'data_nascimento'
        dataframe_unificado = limpar_dados(dataframe_unificado)
        
        # Gerar um novo ID único para cada usuário
        dataframe_unificado['id'] = range(1, len(dataframe_unificado) + 1)

        # Reorganizar as colunas no DataFrame final (reindex)
        nova_ordem = ['id'] + [col for col in dataframe_unificado.columns if col != 'id']
        dataframe_unificado = dataframe_unificado.reindex(columns=nova_ordem)
        
        return dataframe_unificado
    
    except Exception as e:
        print(f">>ERROR: FALHA NA UNIFICAÇÃO<< {str(e)}")
        return pd.DataFrame()

        
def exportar_dados(df, nome_arquivo):
    """
    Exporta o DataFrame para um arquivo Excel.

    Args:
    - df (pandas DataFrame): DataFrame a ser exportado.
    - nome_arquivo (str): Nome do arquivo de saída.

    """
    try:
        df.to_excel(nome_arquivo, index=False, engine='openpyxl')
        print(f'>>SUCESSO: DADOS EXPORTADOS PARA: {nome_arquivo} <<')
    except Exception as e:
        print(f'>>ERROR: FALHA NA EXPORTAÇÃO<< {str(e)}')    
    
def main():
    try:
        # Ler arquivos CSV, JSON e Excel
        df_csv, df_json, df_excel = ler_arquivos()

        # Unificar dados dos DataFrames
        dataframe_unificado = unificar_dados(df_csv, df_json, df_excel)

        # Exportar dados unificados para Excel
        if not dataframe_unificado.empty:
            exportar_dados(dataframe_unificado, 'Usuarios/usuarios_final.xlsx')
        else:
            print(">>NENHUM DADO ENCONTRADO PARA EXPORTAÇÃO<<")

    except Exception as e:
        print(f">>ERROR<< {str(e)}")

if __name__ == "__main__":
    print('>>IMPORTANDO DADOS...ESPERE UM MINUTO...<<')
    main()
