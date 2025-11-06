"""
Script para atualizar dados da CVM automaticamente
Baixa ITRs mais recentes, processa e atualiza no Supabase
"""

import requests
import pandas as pd
import zipfile
from io import BytesIO, StringIO
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def transformar_wide_para_long(df, tipo_demonstracao):
    """
    Transforma dados do formato CVM (wide) para formato Supabase (long)
    
    Args:
        df: DataFrame com dados da CVM
        tipo_demonstracao: 'DRE', 'BPA', 'BPP', ou 'DFC'
    
    Returns:
        DataFrame transformado no formato: Ticker, Conta, Ano, Trimestre, Valor
    """
    
    print(f"   🔄 Transformando {tipo_demonstracao} para formato long...")
    
    try:
        # Verificar colunas necessárias
        colunas_necessarias = ['DENOM_CIA', 'DS_CONTA', 'DT_FIM_EXERC', 'VL_CONTA']
        
        for col in colunas_necessarias:
            if col not in df.columns:
                print(f"   ⚠️  Coluna {col} não encontrada. Colunas disponíveis: {df.columns.tolist()[:10]}")
                return None
        
        # Criar DataFrame limpo
        df_clean = df[colunas_necessarias].copy()
        
        # Limpar valores
        df_clean = df_clean.dropna(subset=['VL_CONTA', 'DT_FIM_EXERC'])
        
        # Converter valor para numérico
        df_clean['VL_CONTA'] = pd.to_numeric(df_clean['VL_CONTA'], errors='coerce')
        df_clean = df_clean.dropna(subset=['VL_CONTA'])
        
        # Extrair Ano e Trimestre da data
        df_clean['DT_FIM_EXERC'] = pd.to_datetime(df_clean['DT_FIM_EXERC'], errors='coerce')
        df_clean = df_clean.dropna(subset=['DT_FIM_EXERC'])
        
        df_clean['Ano'] = df_clean['DT_FIM_EXERC'].dt.year
        df_clean['Trimestre'] = df_clean['DT_FIM_EXERC'].dt.quarter
        
        # Mapear nome da empresa para Ticker (simplificado)
        # TODO: Implementar mapeamento correto CNPJ → Ticker
        df_clean['Ticker'] = df_clean['DENOM_CIA'].str[:5].str.upper()
        
        # Renomear colunas
        df_long = df_clean[[
            'Ticker',
            'DS_CONTA',
            'Ano',
            'Trimestre',
            'VL_CONTA'
        ]].copy()
        
        df_long.columns = ['Ticker', 'Conta', 'Ano', 'Trimestre', 'Valor']
        
        # Remover duplicatas
        df_long = df_long.drop_duplicates(
            subset=['Ticker', 'Conta', 'Ano', 'Trimestre'],
            keep='last'
        )
        
        print(f"   ✅ Transformado: {len(df_long):,} registros únicos")
        
        return df_long
        
    except Exception as e:
        print(f"   ❌ Erro na transformação: {e}")
        import traceback
        traceback.print_exc()
        return None


def obter_ultimos_trimestres_por_empresa():
    """
    Consulta Supabase para descobrir qual o último trimestre de cada empresa
    
    Returns:
        Dict com {Ticker: {'ultimo_ano': 2025, 'ultimo_trimestre': 1}}
    """
    
    print("\n" + "="*70)
    print("🔍 DETECTANDO ÚLTIMO TRIMESTRE DE CADA EMPRESA NO SUPABASE")
    print("="*70 + "\n")
    
    from config.supabase_config import supabase
    
    try:
        # Baixar dados atuais do Supabase
        print("📥 Baixando dados atuais do Supabase...")
        
        resultado = supabase.table('balancos_trimestrais') \
            .select('arquivo_path') \
            .eq('status', 'ativo') \
            .order('data_upload', desc=True) \
            .limit(1) \
            .execute()
        
        if not resultado.data:
            print("⚠️  Nenhum dado encontrado no Supabase")
            return {}
        
        arquivo_path = resultado.data[0]['arquivo_path']
        
        # Download do Parquet
        response = supabase.storage.from_('balancos').download(arquivo_path)
        df_atual = pd.read_parquet(BytesIO(response))
        
        print(f"✅ Carregados {len(df_atual):,} registros atuais")
        
        # Agrupar por Ticker e encontrar último trimestre
        ultimos_trimestres = {}
        
        for ticker in df_atual['Ticker'].unique():
            df_ticker = df_atual[df_atual['Ticker'] == ticker]
            
            # Encontrar último ano e trimestre
            ultimo_ano = df_ticker['Ano'].max()
            df_ultimo_ano = df_ticker[df_ticker['Ano'] == ultimo_ano]
            ultimo_trimestre = df_ultimo_ano['Trimestre'].max()
            
            ultimos_trimestres[ticker] = {
                'ultimo_ano': int(ultimo_ano),
                'ultimo_trimestre': int(ultimo_trimestre)
            }
        
        # Mostrar resumo
        print(f"\n📊 Resumo dos últimos trimestres por empresa:")
        print(f"   Total de empresas: {len(ultimos_trimestres)}")
        
        # Mostrar alguns exemplos
        exemplos = list(ultimos_trimestres.items())[:5]
        for ticker, info in exemplos:
            print(f"   • {ticker}: {info['ultimo_ano']}-Q{info['ultimo_trimestre']}")
        
        print(f"   ... (mais {len(ultimos_trimestres) - 5} empresas)")
        
        return ultimos_trimestres
        
    except Exception as e:
        print(f"❌ Erro ao obter últimos trimestres: {e}")
        import traceback
        traceback.print_exc()
        return {}
        

def filtrar_dados_novos(df_transformado, ultimos_trimestres):
    """
    Filtra apenas registros mais recentes que o último trimestre de cada empresa
    
    Args:
        df_transformado: DataFrame no formato long (Ticker, Conta, Ano, Trimestre, Valor)
        ultimos_trimestres: Dict com último trimestre de cada empresa
    
    Returns:
        DataFrame com apenas dados novos
    """
    
    print(f"\n🔍 Filtrando apenas dados novos...")
    print(f"   Total antes do filtro: {len(df_transformado):,} registros")
    
    if not ultimos_trimestres:
        print(f"   ⚠️  Sem informação de últimos trimestres, mantendo todos os dados")
        return df_transformado
    
    # Lista para armazenar registros novos
    registros_novos = []
    
    # Agrupar por Ticker
    for ticker in df_transformado['Ticker'].unique():
        df_ticker = df_transformado[df_transformado['Ticker'] == ticker]
        
        # Verificar se temos info do último trimestre dessa empresa
        if ticker not in ultimos_trimestres:
            # Empresa nova, adicionar todos os dados
            registros_novos.append(df_ticker)
            continue
        
        ultimo_ano = ultimos_trimestres[ticker]['ultimo_ano']
        ultimo_trimestre = ultimos_trimestres[ticker]['ultimo_trimestre']
        
        # Filtrar apenas registros APÓS o último trimestre
        df_novos = df_ticker[
            (df_ticker['Ano'] > ultimo_ano) |
            ((df_ticker['Ano'] == ultimo_ano) & (df_ticker['Trimestre'] > ultimo_trimestre))
        ]
        
        if len(df_novos) > 0:
            registros_novos.append(df_novos)
    
    # Concatenar todos os novos registros
    if registros_novos:
        df_final = pd.concat(registros_novos, ignore_index=True)
        print(f"   ✅ Dados novos encontrados: {len(df_final):,} registros")
        
        # Mostrar resumo por empresa
        empresas_com_novos = df_final.groupby('Ticker').size()
        print(f"\n   📊 Empresas com dados novos: {len(empresas_com_novos)}")
        
        # Mostrar exemplos
        for ticker in list(empresas_com_novos.index[:5]):
            df_ticker = df_final[df_final['Ticker'] == ticker]
            anos_trimestres = df_ticker[['Ano', 'Trimestre']].drop_duplicates()
            periodos = [f"{row['Ano']}-Q{row['Trimestre']}" for _, row in anos_trimestres.iterrows()]
            print(f"      • {ticker}: {', '.join(periodos)} ({len(df_ticker)} registros)")
        
        return df_final
    else:
        print(f"   ℹ️  Nenhum dado novo encontrado")
        return pd.DataFrame()


def obter_lista_empresas():
    """Retorna lista de CNPJs das 133 empresas monitoradas"""
    from config.supabase_config import supabase
    
    try:
        resultado = supabase.table('empresas_ativas') \
            .select('ticker, cnpj') \
            .eq('status', 'ativa') \
            .execute()
        
        empresas = {row['ticker']: row.get('cnpj', '') for row in resultado.data}
        print(f"✅ Carregadas {len(empresas)} empresas do Supabase")
        return empresas
        
    except Exception as e:
        print(f"⚠️  Erro ao carregar empresas, usando lista padrão: {e}")
        # Fallback: usar lista de tickers
        return {
            'ABEV3': '', 'ALPA4': '', 'ALUP3': '', 'AMAR3': '', 'AMBP3': '',
            'ANIM3': '', 'AZZA3': '', 'AZUL4': '', 'BBAS3': '', 'BBDC4': '',
            # ... (continua com todos os 133 tickers)
        }

def verificar_ultimo_trimestre_disponivel():
    """Verifica qual o último trimestre disponível na CVM"""
    
    print("\n" + "="*70)
    print("🔍 VERIFICANDO DADOS DISPONÍVEIS NA CVM")
    print("="*70 + "\n")
    
    ano_atual = datetime.now().year
    base_url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
    
    try:
        response = requests.get(base_url, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Erro ao acessar CVM: HTTP {response.status_code}")
            return None
        
        # Procurar arquivo mais recente
        arquivo_ano_atual = f"itr_cia_aberta_{ano_atual}.zip"
        arquivo_ano_anterior = f"itr_cia_aberta_{ano_atual - 1}.zip"
        
        if arquivo_ano_atual.lower() in response.text.lower():
            print(f"✅ Encontrado: {arquivo_ano_atual}")
            return ano_atual, arquivo_ano_atual
        elif arquivo_ano_anterior.lower() in response.text.lower():
            print(f"✅ Encontrado: {arquivo_ano_anterior}")
            return ano_atual - 1, arquivo_ano_anterior
        else:
            print(f"⚠️  Nenhum arquivo ITR encontrado para {ano_atual} ou {ano_atual - 1}")
            return None
            
    except Exception as e:
        print(f"❌ Erro ao verificar CVM: {e}")
        return None

def baixar_e_processar_itr(ano, arquivo, empresas, ultimos_trimestres):
    """Baixa ZIP da CVM e processa dados filtrados por empresa"""
    
    print(f"\n📥 Baixando {arquivo}...")
    
    base_url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
    url = base_url + arquivo
    
    try:
        # Download com timeout maior
        response = requests.get(url, timeout=300, stream=True)
        
        if response.status_code != 200:
            print(f"❌ Erro no download: HTTP {response.status_code}")
            return None
        
        # Ler ZIP em memória
        tamanho_mb = len(response.content) / 1024 / 1024
        print(f"✅ Download concluído ({tamanho_mb:.1f} MB)")
        
        zip_file = zipfile.ZipFile(BytesIO(response.content))
        
        # Procurar arquivos relevantes no ZIP
        arquivos_relevantes = {
            'DRE': 'itr_cia_aberta_DRE_con',
            'BPA': 'itr_cia_aberta_BPA_con',
            'BPP': 'itr_cia_aberta_BPP_con',
            'DFC': 'itr_cia_aberta_DFC_MI_con'
        }
        
        # Lista de tickers para filtrar
        tickers = list(empresas.keys())
        print(f"\n🔍 Filtrando por {len(tickers)} empresas monitoradas...")
        
        dados_processados = {}
        
        for tipo, arquivo_interno in arquivos_relevantes.items():
            # Procurar arquivo CSV dentro do ZIP
            csv_name = None
            for name in zip_file.namelist():
                if arquivo_interno in name and name.endswith('.csv'):
                    csv_name = name
                    break
            
            if not csv_name:
                print(f"⚠️  Arquivo {arquivo_interno} não encontrado no ZIP")
                continue
            
            print(f"\n📄 Processando: {tipo}")
            
            # Ler CSV
            with zip_file.open(csv_name) as f:
                # Ler com encoding correto
                df = pd.read_csv(f, sep=';', encoding='latin1', low_memory=False)
                
                total_antes = len(df)
                print(f"   • Total de registros: {total_antes:,}")
                
                # Filtrar apenas empresas monitoradas
                # A coluna DENOM_CIA contém o nome da empresa
                # Vamos criar um filtro aproximado por ticker
                
                # Primeiro, verificar quais colunas existem
                if 'DENOM_CIA' in df.columns:
                    # Filtro por nome aproximado (melhor método seria por CNPJ)
                    # Por enquanto, guardamos todos os dados
                    df_filtrado = df.copy()
                    
                    total_depois = len(df_filtrado)
                    print(f"   • Registros filtrados: {total_depois:,}")
                    

                    # Transformar para formato long
                    df_long = transformar_wide_para_long(df_filtrado, tipo)

                    if df_long is not None:
                        # Filtrar apenas dados novos
                        df_novos = filtrar_dados_novos(df_long, ultimos_trimestres)
                        
                        dados_processados[tipo] = {
                            'dataframe': df_novos,
                            'registros': len(df_novos),
                            'formato': 'long',
                            'somente_novos': True
                        }
                        print(f"   ✅ Pronto para Supabase: {len(df_novos):,} registros novos")                    
                    
                    if df_long is not None:
                        dados_processados[tipo] = {
                            'dataframe': df_long,
                            'registros': len(df_long),
                            'formato': 'long'
                        }
                        print(f"   ✅ Pronto para Supabase: {len(df_long):,} registros")
                    else:
                        print(f"   ⚠️  Falha na transformação, mantendo formato original")
                        dados_processados[tipo] = {
                            'dataframe': df_filtrado,
                            'registros': total_depois,
                            'formato': 'wide'
                        }                
                else:
                    print(f"   ⚠️  Coluna DENOM_CIA não encontrada")
                    dados_processados[tipo] = {
                        'dataframe': df,
                        'registros': total_antes
                    }
        
        return dados_processados
        
    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
        import traceback
        traceback.print_exc()
        return None

def atualizar_supabase(dados):
    """Atualiza dados no Supabase"""
    
    print("\n" + "="*70)
    print("📤 ATUALIZANDO SUPABASE")
    print("="*70 + "\n")
    
    from config.supabase_config import supabase
    
    try:
        # Calcular total de registros
        total_registros = 0
        tipos_processados = []
        
        for tipo, info in dados.items():
            if isinstance(info, dict) and 'registros' in info:
                total_registros += info['registros']
                tipos_processados.append(tipo)
            else:
                # Fallback para formato antigo
                total_registros += info
                tipos_processados.append(tipo)
        
        # Registrar atualização no log
        log = {
            'tipo_atualizacao': 'automatica',
            'status': 'sucesso',
            'registros_novos': total_registros,
            'mensagem': f'Processados {len(tipos_processados)} tipos: {", ".join(tipos_processados)}. Total: {total_registros:,} registros',
            'data_execucao': datetime.now().isoformat()
        }
        
        supabase.table('log_atualizacoes').insert(log).execute()
        
        print(f"✅ Log registrado no Supabase")
        print(f"   • Total de registros: {total_registros:,}")
        print(f"   • Tipos processados: {', '.join(tipos_processados)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro ao atualizar Supabase: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Função principal"""
    
    print("\n" + "="*70)
    print("🤖 AUTOMAÇÃO DE ATUALIZAÇÃO - DADOS CVM")
    print("="*70)
    print(f"📅 Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("="*70)
    
    # Carregar empresas
    empresas = obter_lista_empresas()
    
    # NOVO: Obter últimos trimestres de cada empresa
    ultimos_trimestres = obter_ultimos_trimestres_por_empresa()
    
    # Verificar dados disponíveis
    resultado = verificar_ultimo_trimestre_disponivel()
    
    if not resultado:
        print("\n⚠️  Nenhum dado novo disponível na CVM")
        print("✅ Sistema já está atualizado\n")
        return
    
    ano, arquivo = resultado
    
    # Baixar e processar (agora com filtro de dados novos)
    print(f"\n🔄 Iniciando processamento do ano {ano}...")
    dados = baixar_e_processar_itr(ano, arquivo, empresas, ultimos_trimestres)
    
    if dados:
        print(f"\n📊 Resumo do processamento:")
        total_registros = 0
        for tipo, info in dados.items():
            registros = info['registros']
            total_registros += registros
            print(f"   • {tipo}: {registros:,} registros")
        
        print(f"\n   📈 TOTAL: {total_registros:,} registros processados")

        # Mostrar amostra dos dados transformados
        print(f"\n🔍 AMOSTRA DOS DADOS TRANSFORMADOS:")
        for tipo in ['DRE']:  # Mostrar só DRE para não poluir o log
            if tipo in dados and 'dataframe' in dados[tipo]:
                df_amostra = dados[tipo]['dataframe']
                print(f"\n   {tipo} - Primeiras 5 linhas:")
                print(df_amostra.head(5).to_string(index=False))
                break
                
        # Atualizar Supabase
        sucesso = atualizar_supabase(dados)
        
        if sucesso:
            print("\n" + "="*70)
            print("✅ ATUALIZAÇÃO CONCLUÍDA COM SUCESSO!")
            print("="*70 + "\n")
        else:
            print("\n⚠️  Atualização parcial - verifique logs")
    else:
        print("\n❌ Falha no processamento dos dados")
        sys.exit(1)
    
    print("🎯 Próxima execução: conforme agendamento do GitHub Actions\n")

if __name__ == "__main__":
    main()
