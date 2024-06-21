import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup, FeatureNotFound
import pandas as pd
from io import StringIO

#urls das páginas da Wikipédia
urls = {
    'ps5': 'https://pt.wikipedia.org/wiki/Lista_de_jogos_para_PlayStation_5',
    'ps4': 'https://pt.wikipedia.org/wiki/Lista_de_jogos_para_PlayStation_4',
    'xbox_x_s': 'https://pt.wikipedia.org/wiki/Lista_de_jogos_para_Xbox_Series_X_e_Series_S',
    'xbox360': 'https://pt.wikipedia.org/wiki/Lista_de_jogos_para_Xbox_360',
    'nin_switch': 'https://pt.wikipedia.org/wiki/Lista_de_jogos_para_Nintendo_Switch'
}

def fazer_requisicao(url):
    """
    Faz uma requisição HTTP para a URL fornecida e retorna o conteúdo HTML parseado com BeautifulSoup.
    
    Args:
    - url (str): URL da página a ser requisitada.
    
    Returns:
    - BeautifulSoup object: Objeto BeautifulSoup contendo o conteúdo HTML da página.
    - None: Em caso de erro na requisição.
    """
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()  #lança exceção se a requisição não for bem-sucedida
        
        soup = BeautifulSoup(resposta.text, 'html.parser')
        return soup
        
    except RequestException as e:
        print(f">>ERRO DE REQUISIÇÃO<<: {url}: {e}")
        return None
    except FeatureNotFound as e:
        print(f">>ERRO DO BEAUTIFULSOUP<<: {e}")
        return None
    except Exception as e:
        print(f">>ERRO<<: {e}")

def extrair_tabela(sopa, tipo):
    """
    Extrai a tabela específica de jogos de um console a partir do objeto BeautifulSoup fornecido.
    
    Args:
    - sopa (BeautifulSoup object): Objeto BeautifulSoup da página da Wikipédia.
    - tipo (str): Tipo de tabela ('generico' ou 'ps4').
    
    Returns:
    - BeautifulSoup object: Objeto BeautifulSoup contendo a tabela de jogos.
    - None: Em caso de erro na extração da tabela.
    """
    try:
        if tipo == 'generico':
            tabela = sopa.find('table', id='softwarelist')
        #ps4 nao tem uma table com id 'software list' entao tem que pesquisar pela classe
        elif tipo == 'ps4':
            tabela = sopa.find('table', {'class': 'wikitable sortable'})
        else:
            tabela = None
        
        return tabela
    except AttributeError as e:
        print(f">>ERRO: TABELA NÃO ENCONTRADA<< {e}")
        return None
    except Exception as e:
        print(f">>ERRO<<: {e}")

def limpar_dados_ps5(dataframe):
    """
    Limpa os dados específicos da tabela de jogos do PlayStation 5.
    
    Args:
    - dataframe (pandas DataFrame): DataFrame contendo os dados da tabela de jogos do PS5.
    
    Returns:
    - pandas DataFrame: DataFrame limpo do PS5.
    """
    if dataframe is not None:
        try:
            #retira do dataframe as colunas que nao queremos. level se refere ao level do index, e inplace para nao precisarmos criar outro dataframe
            dataframe.drop(columns=['Addons', 'Ref.'], errors='ignore', inplace=True, level=0)
            
            #remove duplicatas
            dataframe.drop_duplicates(inplace=True)
            
            #preenche campos vazios com "Nao Encontrado"
            dataframe.fillna('Nao Encontrado', inplace=True)
        except Exception as e:
            print(f">>ERRO AO LIMPAR DADOS PS5<<: {e}")
    
    return dataframe

def limpar_dados_ps4(dataframe):
    """
    Limpa os dados específicos da tabela de jogos do PlayStation 4.
    
    Args:
    - dataframe (pandas DataFrame): DataFrame contendo os dados da tabela de jogos do PS4.
    
    Returns:
    - pandas DataFrame: DataFrame limpo do PS4.
    """
    if dataframe is not None:
        try:
            #retira do dataframe as colunas que nao queremos. level se refere ao level do index, e inplace para nao precisarmos criar outro dataframe
            dataframe.drop(columns=['Referências'], errors='ignore', inplace=True)
            
            #remove duplicatas
            dataframe.drop_duplicates(inplace=True)
            
            #preenche campos vazios com "Nao Encontrado"
            dataframe.fillna('Nao Encontrado', inplace=True)
        except Exception as e:
            print(f">>ERRO AO LIMPAR DADOS PS4<<: {e}")
    
    return dataframe

def limpar_dados_xbox_x_s(dataframe):
    """
    Limpa os dados específicos da tabela de jogos do Xbox Series X/S.
    
    Args:
    - dataframe (pandas DataFrame): DataFrame contendo os dados da tabela de jogos do Xbox Series X/S.
    
    Returns:
    - pandas DataFrame: DataFrame limpo do Xbox Series X/S.
    """
    if dataframe is not None:
        try:
            dataframe.drop(columns=['Complementos', 'Ref.'], errors='ignore', level=0, inplace=True)
            dataframe.drop_duplicates(inplace=True)
            dataframe.fillna('Nao Encontrado', inplace=True)
        except Exception as e:
            print(f">>ERRO AO LIMPAR DADOS XBOX X/S<<: {e}")
    
    return dataframe

def limpar_dados_xbox_360(dataframe):
    """
    Limpa os dados específicos da tabela de jogos do Xbox 360.
    
    Args:
    - dataframe (pandas DataFrame): DataFrame contendo os dados da tabela de jogos do Xbox 360.
    
    Returns:
    - pandas DataFrame: DataFrame limpo do Xbox 360.
    """
    if dataframe is not None:
        try:
            dataframe.drop(columns=['Addons', 'Ref.', 'Xbox One'], errors='ignore', level=0, inplace=True)
            dataframe.drop_duplicates(inplace=True)
            dataframe.fillna('Nao Encontrado', inplace=True)
        except Exception as e:
            print(f">>ERRO AO LIMPAR DADOS XBOX 360<<: {e}")
    
    return dataframe

def limpar_dados_nin_switch(dataframe):
    """
    Limpa os dados específicos da tabela de jogos do Nintendo Switch.
    
    Args:
    - dataframe (pandas DataFrame): DataFrame contendo os dados da tabela de jogos do Nintendo Switch.
    
    Returns:
    - pandas DataFrame: DataFrame limpo do Nintendo Switch.
    """
    if dataframe is not None:
        try:
            dataframe.drop(columns=['Obs.', 'Ref.'], errors='ignore', level=0, inplace=True)
            dataframe.drop_duplicates(inplace=True)
            dataframe.fillna('Nao Encontrado', inplace=True)
        except Exception as e:
            print(f">>ERRO AO LIMPAR DADOS NINTENDO SWITCH<<: {e}")
    
    return dataframe

def exportar_dados(dataframe, nome_do_arquivo):
    """
    Exporta os dados de um DataFrame para CSV.
    
    Args:
    - dataframe (pandas DataFrame): DataFrame contendo os dados a serem exportados.
    - nome_do_arquivo (str): Nome base do arquivo de saída (sem extensão).
    """
    try:
        if dataframe is not None:
            dataframe.to_csv(f"Dados_Jogos/{nome_do_arquivo}.csv", index=False)        
            print(f">>DADOS EXPORTADOS<< : {nome_do_arquivo}.csv")
    except Exception as e:
        print(f">>ERRO NO EXPORTAR<< :  {e}") 

def main():
    """
    Função principal que coordena todo o processo de leitura, limpeza e exportação de dados de jogos de consoles.
    """
    print('>>IMPORTANDO DADOS...ESPERE UM MINUTO...<<')
    
    
    #loop nos itens do objeto urls.
    for nome, url in urls.items():
        sopa = fazer_requisicao(url)
        if sopa:
            #tipo é generico se nome não for igual a ps4
            tipo = 'generico' if nome != 'ps4' else 'ps4'
            tabela = extrair_tabela(sopa, tipo)
            
            #cria o dataframe usando a tabela
            dataframe = pd.read_html(StringIO(str(tabela)))[0]
            
            #aplica a função de limpeza correspondente ao tipo de console
            if nome == 'ps5':
                dataframe_limpo = limpar_dados_ps5(dataframe)
            elif nome == 'ps4':
                dataframe_limpo = limpar_dados_ps4(dataframe)
            elif nome == 'xbox_x_s':
                dataframe_limpo = limpar_dados_xbox_x_s(dataframe)
            elif nome == 'xbox360':
                dataframe_limpo = limpar_dados_xbox_360(dataframe)
            elif nome == 'nin_switch':
                dataframe_limpo = limpar_dados_nin_switch(dataframe)
            else:
                dataframe_limpo = None
            
            #armazena o dataframe limpo no dicionário de dataframes
            if dataframe_limpo is not None:
                exportar_dados(dataframe_limpo, nome)
    
if __name__ == "__main__":
    main()
