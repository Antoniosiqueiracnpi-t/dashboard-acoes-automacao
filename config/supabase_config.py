import os
from supabase import create_client, Client

# Credenciais diretas
SUPABASE_URL = "https://iqzkxhcptdpgjnyytbaz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imlxemt4aGNwdGRwZ2pueXl0YmF6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MjM1OTc3MiwiZXhwIjoyMDc3OTM1NzcyfQ.-_dSupP8Z0yVVc0OAbuoCtlRwig8T5P_CoO9Co5omqI"

_supabase_client = None

def get_supabase_client():
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase_client

supabase = get_supabase_client()
