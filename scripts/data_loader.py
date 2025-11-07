"""
Módulo para carregar e processar dados do Supabase
Versão com normalização de contas
"""

import pandas as pd
from io import BytesIO
from config.supabase_config import supabase
from scripts.processador_dados import aplicar_normalizacao, anualizar_trimestres

def carregar_dados_completos():
    """
    Carrega todos os dados disponíveis no Supabase
    Retorna DataFrame normalizado
    """
    try:
        # Buscar arquivo ativo mais recente
        resultado = supabase.table('balancos_trimestrais') \
            .select('arquivo_path') \
            .eq('status', 'ativo') \
            .order('data_upload', desc=True) \
            .limit(1) \
            .execute()
        
        if not resultado.data:
            print("❌ Nenhum arquivo ativo encontrado")
            return None
        
        arquivo_path = resultado.data[0]['arquivo_path']
        
        # Download do Parquet
        response = supabase.storage.from_('balancos').download(arquivo_path)
        df = pd.read_parquet(BytesIO(response))
        
        # Aplicar normalização
        df_normalizado = aplicar_normalizacao(df)
        
        return df_normalizado
        
    except Exception as e:
        print(f"❌ Erro ao carregar dados: {e}")
        return None

def selecionar_empresa(ticker):
    """
    Seleciona dados de uma empresa específica
    Retorna tupla (DataFrame, nome_empresa)
    """
    try:
        df_completo = carregar_dados_completos()
        
        if df_completo is None or df_completo.empty:
            return None, None
        
        # Filtrar por ticker
        df_empresa = df_completo[df_completo['Ticker'] == ticker].copy()
        
        if df_empresa.empty:
            return None, None
        
        # Buscar nome da empresa
        resultado_empresa = supabase.table('empresas_ativas') \
            .select('razao_social') \
            .eq('ticker', ticker) \
            .limit(1) \
            .execute()
        
        nome = resultado_empresa.data[0]['razao_social'] if resultado_empresa.data else ticker
        
        return df_empresa, nome
        
    except Exception as e:
        print(f"❌ Erro ao selecionar empresa: {e}")
        return None, None

def listar_todas_empresas():
    """
    Retorna lista de tickers disponíveis
    """
    try:
        df = carregar_dados_completos()
        
        if df is None or df.empty:
            return []
        
        return sorted(df['Ticker'].unique().tolist())
        
    except Exception as e:
        print(f"❌ Erro ao listar empresas: {e}")
        return []

def obter_dados_anualizados(ticker=None):
    """
    Retorna dados anualizados
    Se ticker for None, retorna para todas as empresas
    """
    try:
        df = carregar_dados_completos()
        
        if df is None or df.empty:
            return None
        
        if ticker:
            df = df[df['Ticker'] == ticker].copy()
        
        df_anual = anualizar_trimestres(df, ticker)
        
        return df_anual
        
    except Exception as e:
        print(f"❌ Erro ao anualizar dados: {e}")
        return None
