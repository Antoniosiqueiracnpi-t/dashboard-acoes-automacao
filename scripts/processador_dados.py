"""
Processador de dados financeiros
Anualiza trimestres e formata valores
"""

import pandas as pd
from scripts.mapeamento_contas import normalizar_nome_conta, classificar_tipo_conta

def anualizar_trimestres(df, ticker=None):
    """
    Converte dados trimestrais em anuais
    DRE: soma dos 4 trimestres
    Balanço: último trimestre do ano
    """
    
    if df is None or df.empty:
        return df
    
    if not all(col in df.columns for col in ['Ticker', 'Conta', 'Ano', 'Trimestre', 'Valor']):
        return df
    
    # Adicionar tipo de conta
    df_work = df.copy()
    df_work['Tipo'] = df_work.apply(
        lambda row: classificar_tipo_conta(row['Conta'], row['Ticker']),
        axis=1
    )
    
    # DRE: somar trimestres
    df_dre = df_work[df_work['Tipo'] == 'DRE'].copy()
    if not df_dre.empty:
        df_dre_anual = df_dre.groupby(['Ticker', 'Conta', 'Ano'], as_index=False)['Valor'].sum()
        df_dre_anual['Trimestre'] = 'Anual'
    else:
        df_dre_anual = pd.DataFrame()
    
    # Balanço: último trimestre
    df_balanco = df_work[df_work['Tipo'].isin(['Ativo', 'Passivo'])].copy()
    if not df_balanco.empty:
        idx = df_balanco.groupby(['Ticker', 'Conta', 'Ano'])['Trimestre'].idxmax()
        df_balanco_anual = df_balanco.loc[idx].copy()
        df_balanco_anual['Trimestre'] = 'Anual'
    else:
        df_balanco_anual = pd.DataFrame()
    
    # Combinar
    if not df_dre_anual.empty and not df_balanco_anual.empty:
        df_anual = pd.concat([df_dre_anual, df_balanco_anual], ignore_index=True)
    elif not df_dre_anual.empty:
        df_anual = df_dre_anual
    elif not df_balanco_anual.empty:
        df_anual = df_balanco_anual
    else:
        df_anual = pd.DataFrame()
    
    return df_anual

def formatar_valor_brasileiro(valor):
    """Formata valor: R$ 2.483.044.000"""
    try:
        if pd.isna(valor) or valor == 0:
            return "R$ 0"
        
        valor = float(valor)
        negativo = valor < 0
        valor_abs = abs(valor)
        
        # Formatar com pontos como separadores de milhar
        valor_fmt = f"{valor_abs:,.0f}".replace(',', '.')
        
        if negativo:
            return f"-R$ {valor_fmt}"
        else:
            return f"R$ {valor_fmt}"
    except:
        return "R$ 0"

def aplicar_normalizacao(df):
    """Aplica normalização de nomes de contas"""
    if df is None or df.empty:
        return df
    
    df_norm = df.copy()
    df_norm['Conta_Original'] = df_norm['Conta']
    df_norm['Conta'] = df_norm.apply(
        lambda row: normalizar_nome_conta(row['Conta'], row.get('Ticker')),
        axis=1
    )
    df_norm['Tipo'] = df_norm.apply(
        lambda row: classificar_tipo_conta(row['Conta'], row.get('Ticker')),
        axis=1
    )
    
    return df_norm

def obter_dados_4_trimestres(df, ticker, ano):
    """Retorna dados dos 4 trimestres de um ano específico"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    df_filtrado = df[
        (df['Ticker'] == ticker) & 
        (df['Ano'] == ano) &
        (df['Trimestre'] != 'Anual')
    ].copy()
    
    return df_filtrado
