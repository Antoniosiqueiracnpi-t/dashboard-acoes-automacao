"""
Dashboard de Análise de Balanços - B3
Versão simplificada e estável
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from scripts.data_loader import carregar_dados_completos, selecionar_empresa, listar_todas_empresas

st.set_page_config(
    page_title="Dashboard B3 - Balanços",
    page_icon="📊",
    layout="wide"
)

# Título
st.title("📊 Dashboard de Análise de Balanços - B3")
st.markdown("**Fonte:** Dados CVM atualizados automaticamente")

# Sidebar
with st.sidebar:
    st.header("⚙️ Configurações")
    
    empresas = listar_todas_empresas()
    
    if not empresas:
        st.error("❌ Erro ao carregar empresas")
        st.stop()
    
    ticker = st.selectbox(
        "Selecione a Empresa",
        empresas,
        index=empresas.index('PETR4') if 'PETR4' in empresas else 0
    )

# Main content
if ticker:
    st.header(f"🏢 {ticker}")
    
    # Carregar dados
    with st.spinner(f"Carregando dados de {ticker}..."):
        df_empresa, nome = selecionar_empresa(ticker)
    
    if df_empresa is None or df_empresa.empty:
        st.error(f"⚠️ Nenhum dado encontrado para {ticker}")
        st.info("Verifique se a empresa está na base de dados")
        st.stop()
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Visão Geral", "📈 Gráficos", "📋 Dados Brutos"])
    
    with tab1:
        st.subheader("Visão Geral da Empresa")
        
        # Métricas
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Trimestres", len(df_empresa.groupby(['Ano', 'Trimestre'])))
        
        with col2:
            st.metric("Primeiro Ano", int(df_empresa['Ano'].min()))
        
        with col3:
            st.metric("Último Ano", int(df_empresa['Ano'].max()))
        
        with col4:
            st.metric("Contas Únicas", df_empresa['Conta'].nunique())
        
        st.markdown("---")
        
        # Tabela de contas mais recentes
        st.subheader("📋 Últimos Dados Disponíveis")
        
        ultimo_ano = df_empresa['Ano'].max()
        ultimo_trim = df_empresa[df_empresa['Ano'] == ultimo_ano]['Trimestre'].max()
        
        df_recente = df_empresa[
            (df_empresa['Ano'] == ultimo_ano) & 
            (df_empresa['Trimestre'] == ultimo_trim)
        ].copy()
        
        if not df_recente.empty:
            st.markdown(f"**Período:** {ultimo_ano} - Q{ultimo_trim}")
            
            # Formatar valores
            df_recente['Valor_Formatado'] = df_recente['Valor'].apply(
                lambda x: f"R$ {x:,.0f}".replace(',', '.')
            )
            
            st.dataframe(
                df_recente[['Conta', 'Valor_Formatado']].head(20),
                use_container_width=True,
                hide_index=True
            )
    
    with tab2:
        st.subheader("📈 Evolução de Contas")
        
        # Seletor de conta
        contas_disponiveis = sorted(df_empresa['Conta'].unique())
        conta_selecionada = st.selectbox("Selecione a Conta", contas_disponiveis)
        
        if conta_selecionada:
            df_conta = df_empresa[df_empresa['Conta'] == conta_selecionada].copy()
            
            # Criar coluna de período
            df_conta['Período'] = df_conta['Ano'].astype(str) + '-Q' + df_conta['Trimestre'].astype(str)
            df_conta = df_conta.sort_values(['Ano', 'Trimestre'])
            
            # Gráfico
            fig = px.line(
                df_conta,
                x='Período',
                y='Valor',
                title=f'Evolução: {conta_selecionada}',
                labels={'Valor': 'Valor (R$)', 'Período': 'Trimestre'}
            )
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Tabela de dados
            st.markdown("**Dados:**")
            df_conta['Valor_Formatado'] = df_conta['Valor'].apply(
                lambda x: f"R$ {x:,.0f}".replace(',', '.')
            )
            st.dataframe(
                df_conta[['Período', 'Valor_Formatado']],
                use_container_width=True,
                hide_index=True
            )
    
    with tab3:
        st.subheader("📋 Todos os Dados")
        
        # Filtros
        col1, col2 = st.columns(2)
        
        with col1:
            anos = sorted(df_empresa['Ano'].unique(), reverse=True)
            ano_filtro = st.selectbox("Ano", ['Todos'] + list(anos))
        
        with col2:
            busca = st.text_input("Buscar Conta", "")
        
        # Aplicar filtros
        df_filtrado = df_empresa.copy()
        
        if ano_filtro != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['Ano'] == ano_filtro]
        
        if busca:
            df_filtrado = df_filtrado[
                df_filtrado['Conta'].str.contains(busca, case=False, na=False)
            ]
        
        # Formatar
        df_filtrado['Valor_Formatado'] = df_filtrado['Valor'].apply(
            lambda x: f"R$ {x:,.0f}".replace(',', '.')
        )
        
        st.dataframe(
            df_filtrado[['Ticker', 'Conta', 'Ano', 'Trimestre', 'Valor_Formatado']].sort_values(
                ['Ano', 'Trimestre'], 
                ascending=False
            ),
            use_container_width=True,
            hide_index=True
        )
        
        # Download
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Download CSV",
            csv,
            f"{ticker}_dados.csv",
            "text/csv"
        )

# Footer
st.markdown("---")
st.markdown("**Fonte:** CVM | Atualização automática via GitHub Actions")
