# scripts/data_loader.py
"""
Funções para carregar dados do Supabase
Substitui as funções que liam do Google Sheets
"""

import pandas as pd
from io import BytesIO
from typing import Tuple, Optional
import sys
sys.path.append('..')
from config.supabase_config import supabase

# Cache em memória (evita downloads repetidos)
_cache_df = None
_cache_timestamp = None

def carregar_dados_completos(force_reload: bool = False) -> pd.DataFrame:
    """
    Carrega todos os dados do Supabase Storage
    
    Args:
        force_reload: Se True, força download (ignora cache)
    
    Returns:
        DataFrame com colunas: Ticker, Conta, Ano, Trimestre, Valor
    """
    global _cache_df, _cache_timestamp
    
    # Usar cache se disponível
    if not force_reload and _cache_df is not None:
        print("✓ Usando dados do cache")
        return _cache_df.copy()
    
    print("📥 Baixando dados do Supabase...")
    
    try:
        # Buscar informações do arquivo mais recente
        resultado = supabase.table('balancos_trimestrais') \
            .select('arquivo_path, arquivo_nome, registros_total') \
            .eq('status', 'ativo') \
            .order('data_upload', desc=True) \
            .limit(1) \
            .execute()
        
        if not resultado.data:
            raise ValueError("Nenhum arquivo ativo encontrado no Supabase")
        
        arquivo_path = resultado.data[0]['arquivo_path']
        arquivo_nome = resultado.data[0]['arquivo_nome']
        registros_esperados = resultado.data[0]['registros_total']
        
        print(f"   Arquivo: {arquivo_nome}")
        print(f"   Registros esperados: {registros_esperados:,}")
        
        # Download do arquivo Parquet
        response = supabase.storage.from_('balancos').download(arquivo_path)
        
        # Ler em memória
        df = pd.read_parquet(BytesIO(response))
        
        # Validar
        if len(df) != registros_esperados:
            print(f"⚠️  Aviso: Registros lidos ({len(df):,}) != esperados ({registros_esperados:,})")
        
        # Atualizar cache
        _cache_df = df
        _cache_timestamp = pd.Timestamp.now()
        
        print(f"✓ Dados carregados: {len(df):,} registros")
        return df.copy()
        
    except Exception as e:
        print(f"❌ Erro ao carregar dados: {e}")
        raise


def selecionar_empresa(ticker: str, tipo_demonstracao: str = 'DRE') -> Tuple[pd.DataFrame, str]:
    """
    Seleciona dados de uma empresa específica
    Mantém compatibilidade com função antiga do código
    
    Args:
        ticker: Código da ação (ex: 'PETR4')
        tipo_demonstracao: Não usado (mantido por compatibilidade)
    
    Returns:
        (DataFrame no formato wide, nome da empresa)
    """
    # Carregar dados
    df_long = carregar_dados_completos()
    
    # Filtrar empresa
    df_empresa = df_long[df_long['Ticker'] == ticker].copy()
    
    if df_empresa.empty:
        return None, None
    
    # Transformar de LONG → WIDE (formato antigo)
    df_wide = df_empresa.pivot_table(
        index='Conta',
        columns=['Ano', 'Trimestre'],
        values='Valor',
        aggfunc='first'
    ).reset_index()
    
    # Renomear colunas para formato YYYY_Q
    new_columns = ['Conta']
    for col in df_wide.columns[1:]:
        if isinstance(col, tuple):
            ano, trim = col
            new_columns.append(f"{ano}_{trim}")
        else:
            new_columns.append(str(col))
    
    df_wide.columns = new_columns
    
    # Nome da empresa (usar ticker como nome por enquanto)
    nome_empresa = ticker
    
    return df_wide, nome_empresa


def selecionar_empresa_trimestral(ticker: str, tipo_demonstracao: str = 'DRE') -> Tuple[pd.DataFrame, str]:
    """
    Alias para manter compatibilidade com código antigo
    """
    return selecionar_empresa(ticker, tipo_demonstracao)


def selecionar_empresa_periodo(ticker: str, tipo_demonstracao: str = 'DRE', periodo: str = 'trimestre') -> Tuple[pd.DataFrame, str]:
    """
    Alias para manter compatibilidade com código antigo
    """
    return selecionar_empresa(ticker, tipo_demonstracao)


def selecionar_balanco_periodo(ticker: str, tipo_balanco: str = 'BPA', periodo: str = 'trimestre') -> Tuple[pd.DataFrame, str]:
    """
    Alias para manter compatibilidade com código antigo
    """
    return selecionar_empresa(ticker, tipo_balanco)


def obter_cnpj_por_codigo(ticker: str) -> Optional[str]:
    """
    Busca CNPJ pelo ticker
    Por enquanto retorna o próprio ticker (CNPJ será adicionado depois)
    """
    try:
        resultado = supabase.table('empresas_ativas') \
            .select('cnpj') \
            .eq('ticker', ticker) \
            .single() \
            .execute()
        
        return resultado.data.get('cnpj') if resultado.data else ticker
    except:
        return ticker


def listar_todas_empresas() -> list:
    """
    Retorna lista de todos os tickers disponíveis
    """
    try:
        resultado = supabase.table('empresas_ativas') \
            .select('ticker') \
            .eq('status', 'ativa') \
            .execute()
        
        return [row['ticker'] for row in resultado.data]
    except Exception as e:
        print(f"❌ Erro ao listar empresas: {e}")
        return []


# =====================================================
# FUNÇÕES DE TESTE
# =====================================================

def testar_conexao():
    """Testa se consegue carregar dados"""
    print("="*60)
    print("🧪 TESTANDO CONEXÃO E LEITURA DE DADOS")
    print("="*60)
    
    try:
        # Teste 1: Listar empresas
        print("\n1️⃣  Listando empresas...")
        empresas = listar_todas_empresas()
        print(f"   ✓ {len(empresas)} empresas encontradas")
        print(f"   Primeiras 5: {empresas[:5]}")
        
        # Teste 2: Carregar dados completos
        print("\n2️⃣  Carregando dados completos...")
        df = carregar_dados_completos()
        print(f"   ✓ {len(df):,} registros carregados")
        
        # Teste 3: Selecionar uma empresa (PETR4)
        print("\n3️⃣  Selecionando PETR4...")
        df_petr4, nome = selecionar_empresa('PETR4')
        if df_petr4 is not None:
            print(f"   ✓ PETR4 encontrada")
            print(f"   Shape: {df_petr4.shape}")
            print(f"   Colunas: {df_petr4.columns.tolist()[:5]}...")
        else:
            print("   ❌ PETR4 não encontrada")
        
        print("\n" + "="*60)
        print("✅ TODOS OS TESTES PASSARAM!")
        print("="*60)
        return True
        
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Executar testes se rodar o arquivo diretamente
    testar_conexao()
