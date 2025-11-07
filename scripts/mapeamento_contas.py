"""
Mapeamento EXATO de contas contábeis da CVM
Empresas Financeiras e Não Financeiras
Baseado na estrutura real dos dados
"""

# ============================================================================
# EMPRESAS NÃO FINANCEIRAS
# ============================================================================

# DRE - Demonstração do Resultado do Exercício (Não Financeiras)
MAPEAMENTO_DRE_NAO_FINANCEIRAS = {
    'Receita Líquida': [
        'Receita de Venda de Bens e/ou Serviços',
        'Receita Líquida de Vendas',
        'Receitas de Vendas'
    ],
    
    'Custo dos Bens/Serviços Vendidos': [
        'Custo dos Bens e/ou Serviços Vendidos',
        'Custo dos Produtos Vendidos',
        'CPV'
    ],
    
    'Resultado Bruto': [
        'Resultado Bruto',
        'Lucro Bruto'
    ],
    
    'Despesas/Receitas Operacionais': [
        'Despesas/Receitas Operacionais',
        'Despesas Operacionais'
    ],
    
    'EBIT': [
        'Resultado Antes do Resultado Financeiro e dos Tributos',
        'EBIT',
        'Resultado Operacional'
    ],
    
    'Resultado Financeiro': [
        'Resultado Financeiro'
    ],
    
    'Resultado Antes dos Tributos': [
        'Resultado Antes dos Tributos sobre o Lucro',
        'LAIR'
    ],
    
    'Imposto de Renda e Contribuição Social': [
        'Imposto de Renda e Contribuição Social sobre o Lucro',
        'IR e CSLL'
    ],
    
    'Resultado Líquido das Operações Continuadas': [
        'Resultado Líquido das Operações Continuadas'
    ],
    
    'Resultado Líquido de Operações Descontinuadas': [
        'Resultado Líquido de Operações Descontinuadas'
    ],
    
    'Lucro Líquido': [
        'Resultado Líquido do Exercício',
        'Lucro Líquido',
        'Resultado Líquido'
    ],
    
    'Lucro por Ação': [
        'Lucro por Ação'
    ]
}

# ATIVO (Não Financeiras)
MAPEAMENTO_ATIVO_NAO_FINANCEIRAS = {
    'Ativo Total': [
        'Ativo Total'
    ],
    
    'Ativo Circulante': [
        'Ativo Circulante Total',
        'Ativo Circulante'
    ],
    
    'Caixa e Equivalentes de Caixa': [
        'Caixa',
        'Caixa e Equivalentes de Caixa'
    ],
    
    'Aplicações Financeiras': [
        'Investimentos de Curto Prazo',
        'Aplicações Financeiras'
    ],
    
    'Contas a Receber': [
        'Contas a Receber Líquidas',
        'Contas a Receber de Clientes',
        'Contas a Receber'
    ],
    
    'Outras Contas a Receber': [
        'Outras Contas a Receber'
    ],
    
    'Estoques': [
        'Estoques'
    ],
    
    'Ativos Biológicos': [
        'Ativos Biológicos'
    ],
    
    'Tributos a Recuperar': [
        'Tributos a Recuperar',
        'Impostos a Recuperar'
    ],
    
    'Despesas Antecipadas': [
        'Despesas Antecipadas'
    ],
    
    'Outros Ativos Circulantes': [
        'Outros Ativos Circulantes',
        'Outros Ativos'
    ],
    
    'Ativo Não Circulante': [
        'Ativos Não Circulantes',
        'Ativo Não Circulante'
    ],
    
    'Ativo Realizável a Longo Prazo': [
        'Ativo Realizável a Longo Prazo',
        'Ativos de Longo Prazo',
        'Investimentos de Longo Prazo'
    ],
    
    'Investimentos': [
        'Investimentos'
    ],
    
    'Imobilizado': [
        'Imobilizado'
    ],
    
    'Intangível': [
        'Intangível',
        'Ativos Intangíveis'
    ]
}

# PASSIVO (Não Financeiras)
MAPEAMENTO_PASSIVO_NAO_FINANCEIRAS = {
    'Passivo Total': [
        'Passivo Total'
    ],
    
    'Passivo Circulante': [
        'Passivo Circulante Total',
        'Passivo Circulante'
    ],
    
    'Obrigações Sociais e Trabalhistas': [
        'Obrigações Sociais e Trabalhistas'
    ],
    
    'Fornecedores': [
        'Fornecedores',
        'Fornecedores e Contas a Pagar'
    ],
    
    'Obrigações Fiscais': [
        'Obrigações Fiscais'
    ],
    
    'Empréstimos e Financiamentos CP': [
        'Empréstimos e Financiamentos',
        'Dívida de Curto e Longo Prazo'
    ],
    
    'Outras Obrigações CP': [
        'Outras Obrigações'
    ],
    
    'Passivo Não Circulante': [
        'Passivo Não Circulante'
    ],
    
    'Empréstimos e Financiamentos LP': [
        'Empréstimos e Financiamentos de Longo Prazo',
        'Dívida de Longo Prazo'
    ],
    
    'Outras Obrigações LP': [
        'Outras Obrigações'
    ],
    
    'Tributos Diferidos': [
        'Tributos Diferidos'
    ],
    
    'Patrimônio Líquido': [
        'Patrimônio Líquido Consolidado',
        'Patrimônio Líquido Total',
        'Patrimônio Líquido',
        'Patrimônio Líquido dos Acionistas'
    ],
    
    'Capital Social': [
        'Capital Social Realizado',
        'Capital Social'
    ],
    
    'Reservas de Lucros': [
        'Reservas de Lucros'
    ],
    
    'Lucros/Prejuízos Acumulados': [
        'Lucros/Prejuízos Acumulados'
    ],
    
    'Patrimônio Líquido dos Controladores': [
        'Patrimônio Líquido dos Controladores'
    ],
    
    'Patrimônio Líquido dos Não Controladores': [
        'Patrimônio Líquido dos Não Controladores'
    ]
}

# DFC - Demonstração do Fluxo de Caixa (Não Financeiras)
MAPEAMENTO_DFC_NAO_FINANCEIRAS = {
    'Fluxo de Caixa Operacional': [
        'Fluxo de Caixa Operacional',
        'Caixa Gerado nas Operações'
    ],
    
    'Receita de Operações': [
        'Receita de Operações'
    ],
    
    'Variações em Ativos e Passivos': [
        'Variações em Ativos e Passivos'
    ],
    
    'Outras Atividades Operacionais': [
        'Outras Atividades Operacionais'
    ],
    
    'Fluxo de Caixa de Investimento': [
        'Fluxo de Caixa de Investimento',
        'Caixa Usado em Investimentos'
    ],
    
    'Fluxo de Caixa de Financiamento': [
        'Fluxo de Caixa de Financiamento',
        'Caixa de Financiamentos'
    ],
    
    'Aumento ou Diminuição de Caixa': [
        'Aumento ou Diminuição de Caixa',
        'Variação de Caixa'
    ],
    
    'Saldo Inicial de Caixa': [
        'Saldo Inicial de Caixa'
    ],
    
    'Saldo Final de Caixa': [
        'Saldo Final de Caixa'
    ],
    
    'Depreciação e Amortização': [
        'Depreciação e Amortização'
    ]
}

# ============================================================================
# EMPRESAS FINANCEIRAS (Bancos e Instituições Financeiras)
# ============================================================================

# DRE (Financeiras)
MAPEAMENTO_DRE_FINANCEIRAS = {
    'Receitas da Intermediação Financeira': [
        'Receitas da Intermediação Financeira'
    ],
    
    'Despesas da Intermediação Financeira': [
        'Despesas da Intermediação Financeira'
    ],
    
    'Resultado Bruto da Intermediação Financeira': [
        'Resultado Bruto da Intermediação Financeira',
        'Resultado Bruto'
    ],
    
    'Outras Receitas/Despesas Operacionais': [
        'Outras Receitas/Despesas Operacionais'
    ],
    
    'Resultado Operacional': [
        'Resultado Operacional'
    ],
    
    'Resultado Não Operacional': [
        'Resultado Não Operacional'
    ],
    
    'Resultado Antes dos Tributos': [
        'Resultado Antes dos Tributos sobre o Lucro'
    ],
    
    'Imposto de Renda e Contribuição Social': [
        'Imposto de Renda e Contribuição Social'
    ],
    
    'Participações/Contribuições Estatutárias': [
        'Participações/Contribuições Estatutárias'
    ],
    
    'Reversão dos Juros sobre Capital Próprio': [
        'Reversão dos Juros sobre Capital Próprio'
    ],
    
    'Lucro Líquido': [
        'Resultado Líquido do Exercício',
        'Lucro Líquido'
    ],
    
    'Lucro por Ação': [
        'Lucro por Ação'
    ]
}

# ATIVO (Financeiras)
MAPEAMENTO_ATIVO_FINANCEIRAS = {
    'Ativo Total': [
        'Ativo Total'
    ],
    
    'Caixa e Equivalentes de Caixa': [
        'Caixa e Equivalentes de Caixa',
        'Caixa e Equivalentes de Caixa (ajustado de 1.01)'
    ],
    
    'Aplicações Financeiras': [
        'Aplicações Financeiras'
    ],
    
    'Empréstimos e Recebíveis': [
        'Empréstimos e Recebíveis'
    ],
    
    'Tributos Diferidos': [
        'Tributos Diferidos'
    ],
    
    'Investimentos': [
        'Investimentos'
    ],
    
    'Imobilizado': [
        'Imobilizado'
    ]
}

# PASSIVO (Financeiras)
MAPEAMENTO_PASSIVO_FINANCEIRAS = {
    'Passivo Total': [
        'Passivo Total'
    ],
    
    'Passivos Financeiros ao Valor Justo': [
        'Passivos Financeiros ao Valor Justo através do Resultado'
    ],
    
    'Outros Passivos Financeiros Designados ao Valor Justo': [
        'Outros Passivos Financeiros Designados ao Valor Justo através do Resultado'
    ],
    
    'Passivos Financeiros ao Custo Amortizado': [
        'Passivos Financeiros ao Custo Amortizado'
    ],
    
    'Provisões': [
        'Provisões'
    ],
    
    'Passivos Fiscais': [
        'Passivos Fiscais'
    ],
    
    'Passivos sobre Ativos Não Correntes': [
        'Passivos sobre Ativos Não Correntes a Venda e Descontinuados',
        'Passivos sobre Ativos Não Correntes a Venda'
    ],
    
    'Patrimônio Líquido': [
        'Patrimônio Líquido',
        'Patrimônio Líquido Total',
        'Patrimônio Líquido dos Acionistas'
    ],
    
    'Capital Social': [
        'Capital Social Realizado'
    ]
}

# ============================================================================
# LISTA DE EMPRESAS FINANCEIRAS (para identificação)
# ============================================================================

EMPRESAS_FINANCEIRAS = [
    'BBAS3', 'BBDC4', 'ITUB4', 'SANB3', 'BPAC3',
    'BPAN4', 'BRSR6'
]

# ============================================================================
# FUNÇÕES DE NORMALIZAÇÃO
# ============================================================================

def eh_empresa_financeira(ticker):
    """Verifica se é empresa do setor financeiro"""
    return ticker in EMPRESAS_FINANCEIRAS

def normalizar_nome_conta(nome_conta, ticker=None):
    """
    Normaliza o nome de uma conta encontrando seu nome padrão
    
    Args:
        nome_conta: Nome da conta como vem da CVM
        ticker: Código da empresa (para identificar se é financeira)
    
    Returns:
        Nome normalizado ou original se não encontrado
    """
    
    # Remover espaços extras
    nome_limpo = ' '.join(nome_conta.split()).strip()
    
    # Determinar mapeamentos a usar
    if ticker and eh_empresa_financeira(ticker):
        mapeamentos = [
            MAPEAMENTO_DRE_FINANCEIRAS,
            MAPEAMENTO_ATIVO_FINANCEIRAS,
            MAPEAMENTO_PASSIVO_FINANCEIRAS,
            MAPEAMENTO_DFC_NAO_FINANCEIRAS  # DFC é igual
        ]
    else:
        mapeamentos = [
            MAPEAMENTO_DRE_NAO_FINANCEIRAS,
            MAPEAMENTO_ATIVO_NAO_FINANCEIRAS,
            MAPEAMENTO_PASSIVO_NAO_FINANCEIRAS,
            MAPEAMENTO_DFC_NAO_FINANCEIRAS
        ]
    
    # Buscar correspondência EXATA primeiro
    for mapeamento in mapeamentos:
        for nome_padrao, variacoes in mapeamento.items():
            if nome_limpo in variacoes:
                return nome_padrao
    
    # Se não encontrou correspondência exata, buscar por similaridade
    for mapeamento in mapeamentos:
        for nome_padrao, variacoes in mapeamento.items():
            for variacao in variacoes:
                if variacao.lower() in nome_limpo.lower():
                    return nome_padrao
    
    # Se não encontrou, retorna o nome original
    return nome_limpo

def classificar_tipo_conta(nome_padrao, ticker=None):
    """
    Classifica uma conta em DRE, Ativo, Passivo ou DFC
    
    Args:
        nome_padrao: Nome padronizado da conta
        ticker: Código da empresa
    
    Returns:
        'DRE', 'Ativo', 'Passivo', 'DFC' ou 'Outro'
    """
    
    if ticker and eh_empresa_financeira(ticker):
        if nome_padrao in MAPEAMENTO_DRE_FINANCEIRAS:
            return 'DRE'
        elif nome_padrao in MAPEAMENTO_ATIVO_FINANCEIRAS:
            return 'Ativo'
        elif nome_padrao in MAPEAMENTO_PASSIVO_FINANCEIRAS:
            return 'Passivo'
    else:
        if nome_padrao in MAPEAMENTO_DRE_NAO_FINANCEIRAS:
            return 'DRE'
        elif nome_padrao in MAPEAMENTO_ATIVO_NAO_FINANCEIRAS:
            return 'Ativo'
        elif nome_padrao in MAPEAMENTO_PASSIVO_NAO_FINANCEIRAS:
            return 'Passivo'
    
    if nome_padrao in MAPEAMENTO_DFC_NAO_FINANCEIRAS:
        return 'DFC'
    
    return 'Outro'

def obter_contas_principais(ticker=None):
    """
    Retorna lista de contas principais para exibição no dashboard
    
    Args:
        ticker: Código da empresa (opcional)
    
    Returns:
        Dict com listas de contas por tipo
    """
    
    if ticker and eh_empresa_financeira(ticker):
        return {
            'DRE': [
                'Receitas da Intermediação Financeira',
                'Resultado Bruto da Intermediação Financeira',
                'Resultado Operacional',
                'Lucro Líquido'
            ],
            'Ativo': [
                'Ativo Total',
                'Caixa e Equivalentes de Caixa',
                'Aplicações Financeiras',
                'Empréstimos e Recebíveis'
            ],
            'Passivo': [
                'Passivo Total',
                'Passivos Financeiros ao Custo Amortizado',
                'Patrimônio Líquido'
            ]
        }
    else:
        return {
            'DRE': [
                'Receita Líquida',
                'Resultado Bruto',
                'EBIT',
                'Lucro Líquido'
            ],
            'Ativo': [
                'Ativo Total',
                'Ativo Circulante',
                'Ativo Não Circulante'
            ],
            'Passivo': [
                'Passivo Total',
                'Passivo Circulante',
                'Passivo Não Circulante',
                'Patrimônio Líquido'
            ]
        }
