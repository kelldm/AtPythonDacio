import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base

# URL base da API do Mercado Livre
url = "https://api.mercadolibre.com/sites/MLB/search"

Base = declarative_base()

# Modelando a tabela 'jogos' no banco de dados Mercado Livre
class Jogo(Base):
    __tablename__ = 'jogos'
            
    id = Column(Integer, primary_key=True)
    nome = Column(String)
    preco = Column(String)
    permalink = Column(String)

def criar_sessao(engine):
    """
    Cria e retorna uma sessão do SQLAlchemy a partir de uma engine.

    Args:
    - engine (sqlalchemy.engine.Engine): Engine de conexão com o banco de dados.

    Returns:
    - sqlalchemy.orm.Session: Sessão criada a partir da engine.
    """
    session = sessionmaker(bind=engine) #bind (ligar) é o parametro que passamos
    return session()

def acessar_banco_jogos():
    """
    Acessa o banco de dados SQLite e retorna um DataFrame com os dados da tabela 'jogos_totais'.

    Returns:
    - pandas.DataFrame: DataFrame contendo os dados da tabela 'jogos_totais' ou None em caso de erro.
    """
    try:
        # Cria a conexão com o banco de dados SQLite
        engine = create_engine(f'sqlite:///jogos.db')

        # Consulta a tabela jogos_totais e cria um DataFrame
        df_jogos_totais = pd.read_sql_table('jogos_totais', con=engine)
        
        return df_jogos_totais

    except Exception as e:
        print(f'>>ERROR: FALHA AO ACESSAR BD: {str(e)} <<')
        return None
    finally:
        # Fecha a conexão com o banco de dados
        engine.dispose()

def consumir_api(dataframe, session):
    """
    Consome a API do Mercado Livre para buscar informações de jogos e salvar no banco de dados.

    Args:
    - dataframe (pandas.DataFrame): DataFrame contendo os jogos a serem buscados na API.
    - session (sqlalchemy.orm.Session): Sessão do SQLAlchemy para interação com o banco de dados.

    Returns:
    - None
    """
    for _, row in dataframe.iterrows():
        # Parâmetros da consulta à API do Mercado Livre
        parametros = {
            'category': 'MLB186456',  
            'q': row['jogo']
        }
        
        try:
            # Requisição à API do Mercado Livre
            resposta = requests.get(url, params=parametros)
            resposta.raise_for_status()
            
            # Extrai dados da resposta JSON
            dados = resposta.json()
            
            # Verifica se há resultados na resposta
            if dados['results']:
                # Pega apenas o primeiro resultado
                resultado = dados['results'][0]
                
                nome = resultado.get('title')
                preco = resultado.get('price')
                permalink = resultado.get('permalink')
                
                # Cria novo objeto na tabela 'jogos'
                jogo = Jogo(nome=nome, preco=str(preco), permalink=permalink)
                
                # Adiciona o objeto à sessão e commita as alterações
                session.add(jogo)
                session.commit()
                print(f'>>SUCESSO AO BUSCAR DADOS PARA O JOGO: {row['jogo']} <<')
            else:
                print(f">> NENHUM RESULTADO ENCONTRADO PARA O JOGO: {row['jogo']} <<")
        
        except requests.exceptions.RequestException as e:
            print(f">>ERROR: FALHA NA SOLICITAÇÃO HTTP: '{row['jogo']}': {str(e)}<<")
        except Exception as e:
            print(f">>ERROR: FALHA AO PROCESSAR '{row['jogo']}': {str(e)}<<")
        
def main():
    """
    Função principal que executa o processo de consulta à API do Mercado Livre e armazenamento no banco de dados.
    """
    try:
        # Cria o banco de dados do Mercado Livre
        engine_mercado_livre = create_engine('sqlite:///mercado_livre.db')
        
        # Cria as tabelas no banco de dados
        Base.metadata.create_all(engine_mercado_livre)
        
        # Cria a sessão do SQLAlchemy
        session_mercado_livre = criar_sessao(engine_mercado_livre)
        
        # Obtém o DataFrame com os jogos totais do banco de dados
        dataframe_jogos = acessar_banco_jogos()
        
        if dataframe_jogos is not None:
            # Consome a API do Mercado Livre e salva no banco de dados
            consumir_api(dataframe_jogos, session_mercado_livre)
        
        # Fecha a sessão do SQLAlchemy
        session_mercado_livre.close()

    except Exception as e:
        print(f"Ocorreu um erro durante a execução do programa: {str(e)}")

if __name__ == "__main__":
    print('>> PROCESSANDO DADOS DO MERCADO LIVRE <<')
    main()
