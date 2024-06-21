import pandas as pd
from sqlalchemy import create_engine

def ler_excel(nome_arquivo):
    """
    Lê um arquivo Excel e retorna um DataFrame.

    Args:
    - nome_arquivo (str): Caminho do arquivo Excel a ser lido.

    Returns:
    - dataframe (pandas DataFrame): DataFrame contendo os dados do arquivo Excel.
      None se ocorrer algum erro durante a leitura.
    """
    try:
        dataframe = pd.read_excel(nome_arquivo, engine='openpyxl')
        return dataframe
    except FileNotFoundError:
        print(f"Erro: O arquivo '{nome_arquivo}' não foi encontrado.")
        return None
    except Exception as e:
        print(f"Erro ao ler o arquivo '{nome_arquivo}': {str(e)}")
        return None

def processar_jogos(dataframe):
    """
    Processa os dados do DataFrame para extrair informações sobre os jogos preferidos.
    
    Args:
    - dataframe (pandas DataFrame): DataFrame contendo a coluna 'jogos_preferidos'.

    Returns:
    - jogos_totais (set): Conjunto com todos os jogos relatados.
    - jogos_um_usuario (set): Conjunto com jogos relatados por apenas um usuário.
    - jogos_max (set): Conjunto com jogos com mais aparições.
    """
    
    #inicializa conjunto de todos os jogos e dicionário de frequência
    jogos_totais = set() #set => constroi uma coleção nao ordenada de de elementos unicos
    frequencia_jogos = {}

    #itera sobre cada linha do dataframe
    for _, row in dataframe.iterrows():
        jogos_usuario = row['jogos_preferidos'].strip('[]').replace("'", "").split(', ')
        jogos_set = set(jogos_usuario)
        
        #atualizar o conjunto de todos os jogos
        jogos_totais.update(jogos_set)

        #atualizar a frequência de cada jogo
        for jogo in jogos_set:
            if jogo in frequencia_jogos:
                frequencia_jogos[jogo] += 1
            else:
                frequencia_jogos[jogo] = 1

    #filtrar jogos relatados por apenas um usuário
    jogos_um_usuario = {jogo for jogo, freq in frequencia_jogos.items() if freq == 1}

    #encontrar jogos com mais aparições
    max_aparicoes = max(frequencia_jogos.values())
    jogos_max = {jogo for jogo, freq in frequencia_jogos.items() if freq == max_aparicoes}

    return jogos_totais, jogos_um_usuario, jogos_max

def exportar_para_sqlite(jogos_totais, jogos_um_usuario, jogos_max):
    """
    Exporta conjuntos de jogos para um banco de dados SQLite.

    Args:
    - jogos_totais (set): Conjunto com todos os jogos relatados.
    - jogos_um_usuario (set): Conjunto com jogos relatados por apenas um usuário.
    - jogos_max (set): Conjunto com jogos com mais aparições.
    - nome_banco (str): Nome do banco de dados SQLite para exportar os dados. Default é 'jogos.db'.

    Returns:
    - None
    """
    try:
        print('>>SALVANDO DADOS...<<')
        #cria o banco
        #cria a engine do SQLAlchemy para o banco de dados SQLite
        engine = create_engine(f'sqlite:///jogos.db')
        
        #criar dataframes a partir dos sets
        dataframe_jogos_totais = pd.DataFrame(list(jogos_totais), columns=['jogo'])
        dataframe_jogos_um_usuario = pd.DataFrame(list(jogos_um_usuario), columns=['jogo'])
        dataframe_jogos_max_aparicoes = pd.DataFrame(list(jogos_max), columns=['jogo'])
        
        #exportar dataframes para SQLite, criando ou substituindo as tabelas
        dataframe_jogos_totais.to_sql('jogos_totais', engine, index=False, if_exists='replace')
        dataframe_jogos_um_usuario.to_sql('jogos_um_usuario', engine, index=False, if_exists='replace')
        dataframe_jogos_max_aparicoes.to_sql('jogos_max_aparicoes', engine, index=False, if_exists='replace')

        print(f'>>SUCESSO: DADOS EXPORTADOS PARA SQLITE: jogos.db <<')
    except Exception as e:
        print(f'>>ERROR: FALHA AO EXPORTAR PARA SQLITE<< {str(e)}')

def main():
    """
    Função principal que executa o processamento dos dados principais.
    Lê o arquivo Excel, processa os jogos preferidos e exporta para um banco de dados SQLite.
    """
    
    #le o excel com dados do usuario
    dataframe = ler_excel('Usuarios/usuarios_final.xlsx')
    if dataframe is not None:
        #processa os dados para conseguir os dados dos jogos
        jogos_totais, jogos_um_usuario, jogos_max = processar_jogos(dataframe)
        
        #salva no banco
        exportar_para_sqlite(jogos_totais, jogos_um_usuario, jogos_max)

if __name__ == "__main__":
    print('>>PROCESSANDO DADOS...<<')
    main()
