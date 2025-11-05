# config/supabase_config.py
"""
Configuração e conexão com Supabase
"""

import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Credenciais (serão lidas do .env)
SUPABASE_URL = os.getenv("https://iqzkxhcptdpgjnyytbaz.supabase.co")
SUPABASE_KEY = os.getenv("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imlxemt4aGNwdGRwZ2pueXl0YmF6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MjM1OTc3MiwiZXhwIjoyMDc3OTM1NzcyfQ.-_dSupP8Z0yVVc0OAbuoCtlRwig8T5P_CoO9Co5omqI")

# Validar credenciais
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError(
        "Credenciais Supabase não configuradas! "
        "Adicione SUPABASE_URL e SUPABASE_KEY no arquivo .env"
    )

# Cliente global (singleton)
_supabase_client = None

def get_supabase_client() -> Client:
    """
    Retorna instância do cliente Supabase (singleton)
    """
    global _supabase_client
    
    if _supabase_client is None:
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    return _supabase_client

# Alias para facilitar imports
supabase = get_supabase_client()
