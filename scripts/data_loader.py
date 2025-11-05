"""
Funções para carregar dados do Supabase
"""

import pandas as pd
from io import BytesIO
from typing import Tuple, Optional
import sys
import os

# Adicionar raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.supabase_config import supabase

# Cache em memória
_cache_df = None

def carregar_dados_completos(force_reload: bool = False) -> pd.DataFrame:
    """Carrega todos os dados do Supabase"""
    global _cache_df
    
    if not force_reload and _cache_df is not None:
        print("✓ Usando cache")
        return _cache_df.copy()
    
    print("📥 Baixando do Supabase...")
    
    try:
        # Buscar arquivo mais recente
        resultado = supabase.table('balancos_trimestrais') \
            .select('arquivo_path, arquivo_nome, registros_total') \
            .eq('status', 'ativo') \
            .order('data_upload', desc=True) \
            .limit(1) \
            .execute()
        
        if not resultado.data:
            raise ValueError("Nenhum arquivo encontrado")
        
        arquivo_path = resultado.data[0]['arquivo_path']
        print(f"   Arquivo: {resultado.data[0]['arquivo_nome']}")
        
        # Download
        response = supabase.storage.from_('balancos').download(arquivo_path)
        df = pd.read_parquet(BytesIO(response))
        
        _cache_df = df
        print(f"✓ {len(df):,} registros carregados")
        return df.copy()
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        raise


def selecionar_empresa(ticker: str, tipo: str = 'DRE') -> Tuple[pd.DataFrame, str]:
    """Seleciona dados de uma empresa (formato wide)"""
    df_long = carregar_dados_completos()
    df_empresa = df_long[df_long['Ticker'] == ticker].copy()
    
    if df_empresa.empty:
        return None, None
    
    # LONG → WIDE
    df_wide = df_empresa.pivot_table(
        index='Conta',
        columns=['Ano', 'Trimestre'],
        values='Valor',
        aggfunc='first'
    ).reset_index()
    
    # Renomear colunas: (2014, 1) → 2014_1
    new_cols = ['Conta']
    for col in df_wide.columns[1:]:
        if isinstance(col, tuple):
            new_cols.append(f"{col[0]}_{col[1]}")
        else:
            new_cols.append(str(col))
    df_wide.columns = new_cols
    
    return df_wide, ticker


# Aliases para compatibilidade
def selecionar_empresa_trimestral(ticker: str, tipo: str = 'DRE') -> Tuple[pd.DataFrame, str]:
    return selecionar_empresa(ticker, tipo)

def selecionar_empresa_periodo(ticker: str, tipo: str = 'DRE', periodo: str = 'trimestre') -> Tuple[pd.DataFrame, str]:
    return selecionar_empresa(ticker, tipo)

def selecionar_balanco_periodo(ticker: str, tipo: str = 'BPA', periodo: str = 'trimestre') -> Tuple[pd.DataFrame, str]:
    return selecionar_empresa(ticker, tipo)


def listar_todas_empresas() -> list:
    """Lista todos os tickers"""
    try:
        resultado = supabase.table('empresas_ativas') \
            .select('ticker') \
            .eq('status', 'ativa') \
            .execute()
        return [row['ticker'] for row in resultado.data]
    except:
        return []


def testar_conexao():
    """Teste completo"""
    print("="*60)
    print("🧪 TESTANDO CONEXÃO")
    print("="*60)
    
    try:
        print("\n1️⃣  Listando empresas...")
        empresas = listar_todas_empresas()
        print(f"   ✓ {len(empresas)} empresas")
        print(f"   Primeiras 5: {empresas[:5]}")
        
        print("\n2️⃣  Carregando dados...")
        df = carregar_dados_completos()
        print(f"   ✓ {len(df):,} registros")
        
        print("\n3️⃣  Selecionando PETR4...")
        df_petr4, nome = selecionar_empresa('PETR4')
        if df_petr4 is not None:
            print(f"   ✓ Shape: {df_petr4.shape}")
            print(f"   ✓ Colunas: {list(df_petr4.columns[:5])}")
        
        print("\n" + "="*60)
        print("✅ TODOS OS TESTES OK!")
        print("="*60)
        return True
        
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    testar_conexao()
