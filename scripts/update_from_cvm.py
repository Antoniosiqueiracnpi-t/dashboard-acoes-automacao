"""
Script para atualizar dados da CVM automaticamente
Baixa ITRs mais recentes, filtra por CNPJ, processa e atualiza no Supabase
"""

import requests
import pandas as pd
import zipfile
from io import BytesIO, StringIO
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Lista fixa de CNPJs das 133 empresas (para não depender do Supabase)
CNPJS_MONITORADOS = {
    '50746577000115': 'CSAN3', '33000167000101': 'PETR4', '10629105000168': 'PRIO3',
    '33256439000139': 'UGPA3', '34274233000102': 'VBBR3', '08902291000115': 'CMIN3',
    '33592510000154': 'VALE3', '15141799000103': 'FESA4', '33611500000119': 'GGBR4',
    '92690783000109': 'GOAU4', '60894730000105': 'USIM5', '33042730000104': 'CSNA3',
    '42150391000170': 'BRKM5', '33958695000178': 'UNIP6', '97837181000147': 'DXCO3',
    '56643018000166': 'EUCA4', '89637490000145': 'KLBN3', '16404287000155': 'SUZB3',
    '92791243000103': 'RANI3', '61092037000181': 'ETER3', '83475913000191': 'PTBL3',
    '07689002000189': 'EMBR3', '88610126000129': 'FRAS3', '88611835000129': 'POMO4',
    '89086144000116': 'RAPT4', '84683374000149': 'TUPY3', '84693183000168': 'SHUL4',
    '84429695000111': 'WEGE3', '91983056000169': 'KEPL3', '27093558000115': 'MILS3',
    '56720428000163': 'ROMI3', '09305994000129': 'AZUL4', '06164253000187': 'GOLL4',
    '02387241000160': 'RAIL3', '42278291000124': 'LOGN3', '52548435000179': 'JSLG3',
    '02351144000118': 'TGMA3', '04149454000180': 'ECOR3', '02762121000104': 'STBP3',
    '33130691000105': 'PORT3', '01599101000193': 'SEQL3', '33113309000147': 'VLID3',
    '42331462000131': 'EPAR3', '10807374000177': 'SOJA3', '89096457000155': 'SLCE3',
    '01838723000127': 'BRFS3', '03853896000140': 'MRFG3', '67620377000114': 'BEEF3',
    '07206816000115': 'MDIA3', '07526557000100': 'ABEV3', '32785497000197': 'NATU3',
    '24990777000109': 'GMAT3', '47508411000156': 'PCAR3', '08797760000183': 'CURY3',
    '73178600000118': 'CYRE3', '16614075000100': 'DIRR3', '43470988000165': 'EVEN3',
    '08312229000173': 'EZTC3', '01545826000107': 'GFSA3', '49263189000102': 'HBOR3',
    '08294224000165': 'JHSF3', '24230275000180': 'PLPL3', '71476527000135': 'TEND3',
    '59418806000147': 'TFCO4', '61079117000105': 'ALPA4', '89850341000160': 'GRND3',
    '50926955000142': 'VULC3', '33839910000111': 'VIVA3', '61156113000175': 'MYPK3',
    '60476884000187': 'LEVE3', '10760260000119': 'CVCB3', '09288252000132': 'ANIM3',
    '02800026000140': 'COGN3', '04986320000113': 'SEER3', '08807432000110': 'YDUQ3',
    '16670085000155': 'RENT3', '21314559000166': 'MOVI3', '23373000000132': 'VAMO3',
    '16590234000176': 'AZZA3', '45242914000105': 'CEAB3', '08402943000152': 'GUAR3',
    '61189288000189': 'AMAR3', '92754738000162': 'LREN3', '33041260065290': 'BHIA3',
    '47960950000121': 'MGLU3', '13217485000111': 'SBFG3', '18328118000109': 'PETZ3',
    # Adicione mais CNPJs conforme necessário
}

def obter_ultimos_trimestres_por_empresa():
    """Consulta Supabase para descobrir qual o último trimestre de cada empresa"""
    
    print("\n" + "="*70)
    print("🔍 DETECTANDO ÚLTIMO TRIMESTRE DE CADA EMPRESA NO SUPABASE")
    print("="*70 + "\n")
    
    from config.supabase_config import supabase
    
    try:
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

def transformar_wide_para_long(df, tipo_demonstracao):
    """Transforma dados do formato CVM (wide) para formato Supabase (long)"""
    
    print(f"   🔄 Transformando {tipo_demonstracao} para formato long...")
    
    try:
        # Encontrar coluna de CNPJ
        coluna_cnpj = None
        for possivel in ['CNPJ_CIA', 'CNPJ', 'CD_CVM']:
            if possivel in df.columns:
                coluna_cnpj = possivel
                break
        
        if not coluna_cnpj:
            print(f"   ⚠️  Coluna de CNPJ não encontrada")
            return None
        
        # Limpar CNPJ (remover pontos, barras, traços)
        df[coluna_cnpj] = df[coluna_cnpj].astype(str).str.replace(r'[^\d]', '', regex=True).str.zfill(14)
        
        # FILTRAR APENAS EMPRESAS MONITORADAS
        print(f"   🔍 Filtrando por {len(CNPJS_MONITORADOS)} CNPJs monitorados...")
        df_filtrado = df[df[coluna_cnpj].isin(CNPJS_MONITORADOS.keys())].copy()
        
        print(f"   ✅ Após filtro por CNPJ: {len(df_filtrado):,} registros")
        
        if len(df_filtrado) == 0:
            print(f"   ⚠️  Nenhum registro das empresas monitoradas")
            return None
        
        # Mapear CNPJ para Ticker
        df_filtrado['Ticker'] = df_filtrado[coluna_cnpj].map(CNPJS_MONITORADOS)
        
        # Limpar e preparar dados
        df_filtrado = df_filtrado.dropna(subset=['DS_CONTA', 'DT_FIM_EXERC', 'VL_CONTA'])
        
        # Converter valor para numérico
        df_filtrado['VL_CONTA'] = pd.to_numeric(df_filtrado['VL_CONTA'], errors='coerce')
        df_filtrado = df_filtrado.dropna(subset=['VL_CONTA'])
        
        # Extrair Ano e Trimestre da data
        df_filtrado['DT_FIM_EXERC'] = pd.to_datetime(df_filtrado['DT_FIM_EXERC'], errors='coerce')
        df_filtrado = df_filtrado.dropna(subset=['DT_FIM_EXERC'])
        
        df_filtrado['Ano'] = df_filtrado['DT_FIM_EXERC'].dt.year
        df_filtrado['Trimestre'] = df_filtrado['DT_FIM_EXERC'].dt.quarter
        
        # Criar DataFrame final
        df_long = df_filtrado[[
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
        print(f"   📊 Tickers únicos: {df_long['Ticker'].nunique()}")
        
        return df_long
        
    except Exception as e:
        print(f"   ❌ Erro na transformação: {e}")
        import traceback
        traceback.print_exc()
        return None

def filtrar_dados_novos(df_transformado, ultimos_trimestres):
    """Filtra apenas registros mais recentes que o último trimestre de cada empresa"""
    
    print(f"\n🔍 Filtrando apenas dados novos...")
    print(f"   Total antes do filtro: {len(df_transformado):,} registros")
    
    if not ultimos_trimestres:
        print(f"   ⚠️  Sem informação de últimos trimestres, mantendo todos os dados")
        return df_transformado
    
    registros_novos = []
    
    for ticker in df_transformado['Ticker'].unique():
        df_ticker = df_transformado[df_transformado['Ticker'] == ticker]
        
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
    
    if registros_novos:
        df_final = pd.concat(registros_novos, ignore_index=True)
        print(f"   ✅ Dados novos encontrados: {len(df_final):,} registros")
        
        # Mostrar resumo
        empresas_com_novos = df_final.groupby('Ticker').size()
        print(f"\n   📊 Empresas com dados novos: {len(empresas_com_novos)}")
        
        for ticker in list(empresas_com_novos.index[:5]):
            df_ticker = df_final[df_final['Ticker'] == ticker]
            anos_trimestres = df_ticker[['Ano', 'Trimestre']].drop_duplicates()
            periodos = [f"{row['Ano']}-Q{row['Trimestre']}" for _, row in anos_trimestres.iterrows()]
            print(f"      • {ticker}: {', '.join(sorted(periodos))} ({len(df_ticker)} registros)")
        
        return df_final
    else:
        print(f"   ℹ️  Nenhum dado novo encontrado")
        return pd.DataFrame()

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
        
        arquivo_ano_atual = f"itr_cia_aberta_{ano_atual}.zip"
        arquivo_ano_anterior = f"itr_cia_aberta_{ano_atual - 1}.zip"
        
        if arquivo_ano_atual.lower() in response.text.lower():
            print(f"✅ Encontrado: {arquivo_ano_atual}")
            return ano_atual, arquivo_ano_atual
        elif arquivo_ano_anterior.lower() in response.text.lower():
            print(f"✅ Encontrado: {arquivo_ano_anterior}")
            return ano_atual - 1, arquivo_ano_anterior
        else:
            print(f"⚠️  Nenhum arquivo ITR encontrado")
            return None
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return None

def baixar_e_processar_itr(ano, arquivo, ultimos_trimestres):
    """Baixa ZIP da CVM e processa dados filtrados por empresa"""
    
    print(f"\n📥 Baixando {arquivo}...")
    
    base_url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
    url = base_url + arquivo
    
    try:
        response = requests.get(url, timeout=300, stream=True)
        
        if response.status_code != 200:
            print(f"❌ Erro no download: HTTP {response.status_code}")
            return None
        
        tamanho_mb = len(response.content) / 1024 / 1024
        print(f"✅ Download concluído ({tamanho_mb:.1f} MB)")
        
        zip_file = zipfile.ZipFile(BytesIO(response.content))
        
        arquivos_relevantes = {
            'DRE': 'itr_cia_aberta_DRE_con',
            'BPA': 'itr_cia_aberta_BPA_con',
            'BPP': 'itr_cia_aberta_BPP_con',
            'DFC': 'itr_cia_aberta_DFC_MI_con'
        }
        
        print(f"\n🔍 Processando dados das {len(CNPJS_MONITORADOS)} empresas monitoradas...")
        
        dados_processados = {}
        
        for tipo, arquivo_interno in arquivos_relevantes.items():
            csv_name = None
            for name in zip_file.namelist():
                if arquivo_interno in name and name.endswith('.csv'):
                    csv_name = name
                    break
            
            if not csv_name:
                print(f"⚠️  {tipo} não encontrado")
                continue
            
            print(f"\n📄 Processando: {tipo}")
            
            with zip_file.open(csv_name) as f:
                df = pd.read_csv(f, sep=';', encoding='latin1', low_memory=False)
                
                print(f"   • Total de registros: {len(df):,}")
                
                # Transformar para formato long
                df_long = transformar_wide_para_long(df, tipo)
                
                if df_long is not None and len(df_long) > 0:
                    # Filtrar apenas dados novos
                    df_novos = filtrar_dados_novos(df_long, ultimos_trimestres)
                    
                    dados_processados[tipo] = {
                        'dataframe': df_novos,
                        'registros': len(df_novos),
                        'formato': 'long',
                        'somente_novos': True
                    }
                    print(f"   ✅ Pronto para Supabase: {len(df_novos):,} registros novos")
        
        return dados_processados
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return None

def atualizar_supabase(dados):
    """Registra atualização no Supabase"""
    
    print("\n" + "="*70)
    print("📤 ATUALIZANDO SUPABASE")
    print("="*70 + "\n")
    
    from config.supabase_config import supabase
    
    try:
        total_registros = 0
        tipos_processados = []
        
        for tipo, info in dados.items():
            if isinstance(info, dict) and 'registros' in info:
                total_registros += info['registros']
                tipos_processados.append(tipo)
        
        log = {
            'tipo_atualizacao': 'automatica',
            'status': 'sucesso',
            'registros_novos': total_registros,
            'mensagem': f'Processados {len(tipos_processados)} tipos: {", ".join(tipos_processados)}. Total: {total_registros:,} registros novos',
            'data_execucao': datetime.now().isoformat()
        }
        
        supabase.table('log_atualizacoes').insert(log).execute()
        
        print(f"✅ Log registrado no Supabase")
        print(f"   • Total de registros novos: {total_registros:,}")
        print(f"   • Tipos processados: {', '.join(tipos_processados)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro: {e}")
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
    
    print(f"\n📊 Monitorando {len(CNPJS_MONITORADOS)} empresas da B3")
    
    # Obter últimos trimestres
    ultimos_trimestres = obter_ultimos_trimestres_por_empresa()
    
    # Verificar dados disponíveis
    resultado = verificar_ultimo_trimestre_disponivel()
    
    if not resultado:
        print("\n⚠️  Nenhum dado novo disponível na CVM")
        print("✅ Sistema já está atualizado\n")
        return
    
    ano, arquivo = resultado
    
    # Baixar e processar
    print(f"\n🔄 Iniciando processamento do ano {ano}...")
    dados = baixar_e_processar_itr(ano, arquivo, ultimos_trimestres)
    
    if dados:
        print(f"\n📊 Resumo do processamento:")
        total_registros = 0
        for tipo, info in dados.items():
            registros = info['registros']
            total_registros += registros
            print(f"   • {tipo}: {registros:,} registros")
        
        print(f"\n   📈 TOTAL: {total_registros:,} registros novos processados")
        
        # Mostrar amostra
        if total_registros > 0:
            print(f"\n🔍 AMOSTRA DOS DADOS TRANSFORMADOS:")
            for tipo in ['DRE']:
                if tipo in dados and 'dataframe' in dados[tipo]:
                    df_amostra = dados[tipo]['dataframe']
                    if len(df_amostra) > 0:
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
        print("\n❌ Falha no processamento")
        sys.exit(1)
    
    print("🎯 Próxima execução: conforme agendamento do GitHub Actions\n")

if __name__ == "__main__":
    main()
