import os
from supabase import create_client, Client

# Ler variáveis de ambiente
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Debug: verificar se as variáveis foram carregadas
if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL não está configurada nas variáveis de ambiente")

if not SUPABASE_KEY:
    raise ValueError("SUPABASE_KEY não está configurada nas variáveis de ambiente")

# Validar formato da URL
if not SUPABASE_URL.startswith("https://"):
    raise ValueError(f"SUPABASE_URL inválida: {SUPABASE_URL}")

print(f"🔗 Conectando ao Supabase: {SUPABASE_URL}")

_supabase_client = None

def get_supabase_client():
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Cliente Supabase criado com sucesso")
    return _supabase_client

supabase = get_supabase_client()
