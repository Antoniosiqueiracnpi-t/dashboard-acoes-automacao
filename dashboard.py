"""
Dashboard de Análise de Balanços - B3
Visualização de dados financeiros de 86 empresas
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from scripts.data_loader import (
    carregar_dados_completos, 
    selecionar_empresa,
    listar_todas_empresas
)

# Configuração da página
st.set_page_config(
    page_title="Dashboard B3 - Balanços",
    page_icon="📊",
    layout="wide"
)

# Título
st.title("📊 Dashboard de Análise de Balanços - B3")
st.markdown("Dados financeiros atualizados automaticamente toda segunda-feira")

# Sidebar
with st.sidebar:
    st.header("⚙️ Configurações")
    
    # Seleção de empresa
    empresas = listar_todas_empresas()
    
    if not empresas:
        st.error("❌ Erro ao carregar lista de empresas")
        st.stop()
    
    ticker = st.selectbox(
        "Selecione a Empresa",
        empresas,
        index=empresas.index('PETR4') if 'PETR4' in empresas else 0
    )
    
    st.markdown("---")
    st.markdown("### 📈 Estatísticas do Sistema")
    
    # Carregar dados completos para estatísticas
    with st.spinner("Carregando dados..."):
        df_completo = carregar_dados_completos()
    
    if df_completo is not None and not df_completo.empty:
        st.metric("Total de Registros", f"{len(df_completo):,}")
        st.metric("Empresas", df_completo['Ticker'].nunique())
        st.metric("Último Ano", df_completo['Ano'].max())

# Main content
if ticker:
    st.header(f"🏢 {ticker}")
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Visão Geral", 
        "📈 DRE", 
        "💰 Balanço",
        "📋 Dados Brutos"
    ])
    
    with tab1:
        st.subheader("Visão Geral da Empresa")
        
        # Carregar dados da empresa
        with st.spinner(f"Carregando dados de {ticker}..."):
            df_empresa, nome = selecionar_empresa(ticker)
        
        if df_empresa is None or df_empresa.empty:
            st.warning(f"⚠️ Nenhum dado encontrado para {ticker}")
            st.stop()
        
        # Filtrar apenas dados da empresa selecionada
        df_ticker = df_completo[df_completo['Ticker'] == ticker].copy()
        
        # Métricas principais
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Trimestres Disponíveis",
                len(df_ticker.groupby(['Ano', 'Trimestre']))
            )
        
        with col2:
            st.metric(
                "Primeiro Ano",
                int(df_ticker['Ano'].min())
            )
        
        with col3:
            st.metric(
                "Último Ano",
                int(df_ticker['Ano'].max())
            )
        
        with col4:
            st.metric(
                "Total de Contas",
                df_ticker['Conta'].nunique()
            )
        
        st.markdown("---")
        
        # Gráfico: Evolução de Receita Líquida
        st.subheader("📈 Evolução da Receita Líquida")
        
        # Buscar receita líquida
        df_receita = df_ticker[
            df_ticker['Conta'].str.contains('Receita', case=False, na=False)
        ].copy()
        
        if not df_receita.empty:
            # Criar coluna de período
            df_receita['Período'] = df_receita['Ano'].astype(str) + '-Q' + df_receita['Trimestre'].astype(str)
            
            # Agrupar por período
            df_plot = df_receita.groupby('Período')['Valor'].sum().reset_index()
            
            fig = px.line(
                df_plot,
                x='Período',
                y='Valor',
                title='Evolução da Receita por Trimestre',
                labels={'Valor': 'Receita (R$)', 'Período': 'Trimestre'}
            )
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("ℹ️ Dados de receita não encontrados")
    
    with tab2:
        st.subheader("📈 Demonstração do Resultado do Exercício (DRE)")
        
        # Filtrar contas principais da DRE
        contas_dre = [
            'Receita', 'Custo', 'Lucro Bruto', 'EBITDA', 
            'Lucro', 'Resultado', 'Despesa'
        ]
        
        df_dre = df_ticker[
            df_ticker['Conta'].str.contains('|'.join(contas_dre), case=False, na=False)
        ].copy()
        
        if not df_dre.empty:
            # Seletor de período
            anos = sorted(df_dre['Ano'].unique(), reverse=True)
            ano_selecionado = st.selectbox("Selecione o Ano", anos)
            
            df_ano = df_dre[df_dre['Ano'] == ano_selecionado]
            
            # Mostrar por trimestre
            col1, col2, col3, col4 = st.columns(4)
            
            for idx, trimestre in enumerate([1, 2, 3, 4]):
                df_trim = df_ano[df_ano['Trimestre'] == trimestre]
                
                with [col1, col2, col3, col4][idx]:
                    st.markdown(f"### Q{trimestre}/{ano_selecionado}")
                    
                    if not df_trim.empty:
                        for _, row in df_trim.head(5).iterrows():
                            valor_fmt = f"R$ {row['Valor']:,.0f}"
                            st.markdown(f"**{row['Conta'][:30]}**")
                            st.markdown(f"{valor_fmt}")
                            st.markdown("---")
                    else:
                        st.info("Sem dados")
        else:
            st.warning("⚠️ Dados de DRE não encontrados")
    
    with tab3:
        st.subheader("💰 Balanço Patrimonial")
        
        # Filtrar contas de balanço
        contas_balanco = ['Ativo', 'Passivo', 'Patrimônio']
        
        df_balanco = df_ticker[
            df_ticker['Conta'].str.contains('|'.join(contas_balanco), case=False, na=False)
        ].copy()
        
        if not df_balanco.empty:
            anos = sorted(df_balanco['Ano'].unique(), reverse=True)
            ano_balanco = st.selectbox("Ano", anos, key='balanco')
            
            df_ano_balanco = df_balanco[df_balanco['Ano'] == ano_balanco]
            
            # Comparação trimestral
            trimestres = sorted(df_ano_balanco['Trimestre'].unique())
            
            if len(trimestres) >= 2:
                trim1, trim2 = st.columns(2)
                
                with trim1:
                    st.markdown(f"### Q{trimestres[-2]}/{ano_balanco}")
                    df_t1 = df_ano_balanco[df_ano_balanco['Trimestre'] == trimestres[-2]]
                    st.dataframe(
                        df_t1[['Conta', 'Valor']].head(10),
                        use_container_width=True,
                        hide_index=True
                    )
                
                with trim2:
                    st.markdown(f"### Q{trimestres[-1]}/{ano_balanco}")
                    df_t2 = df_ano_balanco[df_ano_balanco['Trimestre'] == trimestres[-1]]
                    st.dataframe(
                        df_t2[['Conta', 'Valor']].head(10),
                        use_container_width=True,
                        hide_index=True
                    )
        else:
            st.warning("⚠️ Dados de balanço não encontrados")
    
    with tab4:
        st.subheader("📋 Dados Brutos")
        
        # Filtros
        col1, col2 = st.columns(2)
        
        with col1:
            anos_disponiveis = sorted(df_ticker['Ano'].unique(), reverse=True)
            ano_filtro = st.selectbox("Filtrar por Ano", ['Todos'] + list(anos_disponiveis))
        
        with col2:
            contas_disponiveis = sorted(df_ticker['Conta'].unique())
            busca_conta = st.text_input("Buscar Conta", "")
        
        # Aplicar filtros
        df_filtrado = df_ticker.copy()
        
        if ano_filtro != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['Ano'] == ano_filtro]
        
        if busca_conta:
            df_filtrado = df_filtrado[
                df_filtrado['Conta'].str.contains(busca_conta, case=False, na=False)
            ]
        
        # Mostrar dados
        st.dataframe(
            df_filtrado[['Ticker', 'Conta', 'Ano', 'Trimestre', 'Valor']].sort_values(
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
            "text/csv",
            key='download-csv'
        )

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p>Dashboard atualizado automaticamente via GitHub Actions</p>
    <p>Dados: CVM | Atualização: Toda segunda-feira às 6h UTC</p>
</div>
""", unsafe_allow_html=True)
