import psycopg2
from psycopg2.extras import RealDictCursor
import csv
import re
import os
from dotenv import load_dotenv  # <--- IMPORTANTE

# Carrega as variáveis do arquivo .env
load_dotenv()

# --- CONFIGURAÇÃO DAS CONEXÕES VIA ENV ---
DB_GESTAO = {
    'host': os.getenv('DB_GESTAO_HOST'),
    'database': os.getenv('DB_GESTAO_NAME'),
    'user': os.getenv('DB_GESTAO_USER'),
    'password': os.getenv('DB_GESTAO_PASS')
}

DB_CONTRATO = {
    'host': os.getenv('DB_CONTRATO_HOST'),
    'database': os.getenv('DB_CONTRATO_NAME'),
    'user': os.getenv('DB_CONTRATO_USER'),
    'password': os.getenv('DB_CONTRATO_PASS')
}

DB_PESSOA = {
    'host': os.getenv('DB_PESSOA_HOST'),
    'database': os.getenv('DB_PESSOA_NAME'),
    'user': os.getenv('DB_PESSOA_USER'),
    'password': os.getenv('DB_PESSOA_PASS')
}

SENHA_ACCOUNTS = os.getenv('DB_ACCOUNTS_PASS')

# --- FUNÇÕES AUXILIARES ---
def limpar_cpf(cpf):
    """Remove caracteres não numéricos."""
    if not cpf: return None
    return re.sub(r'\D', '', str(cpf))

def formatar_cpf(cpf):
    """Aplica máscara de CPF (apenas para exibição se necessário)."""
    c = limpar_cpf(cpf)
    if len(c) != 11: return c
    return f"{c[:3]}.{c[3:6]}.{c[6:9]}-{c[9:]}"

def salvar_csv(nome_arquivo, dados, cabecalho):
    """Salva lista de dicionários em CSV na raiz."""
    caminho = os.path.join(os.getcwd(), nome_arquivo)
    if not dados:
        print(f"-> Arquivo {nome_arquivo} não gerado (sem dados).")
        return

    try:
        with open(caminho, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=cabecalho)
            writer.writeheader()
            writer.writerows(dados)
        print(f"-> Relatório salvo: {caminho} ({len(dados)} registros)")
    except Exception as e:
        print(f"Erro ao salvar {nome_arquivo}: {e}")

def main():
    print("--- INICIANDO DIAGNÓSTICO DE DIVERGÊNCIAS ---")
    
    # LISTAS PARA RELATÓRIOS
    lista_email_duplicado = []
    lista_um_inexistente = []
    lista_ambos_inexistentes = []
    lista_erros_outros = []

    # 1. BUSCAR DIVERGÊNCIAS (GESTAO + ACCOUNTS via DBLINK)
    print("[1/4] Buscando divergências iniciais...")
    divergencias = []
    
    sql_base = """
    WITH divergencias AS (
        SELECT
           a.id AS id_account,
           s.sso_id AS sso_id_gestao,
           a.cpf_cnpj AS cpf_visual_accounts,
           s.cpf_cnpj AS cpf_visual_gestao,
           REGEXP_REPLACE(a.cpf_cnpj, '\D','', 'g') AS cpf_accounts_limpo, 
           REGEXP_REPLACE(s.cpf_cnpj, '\D','', 'g') AS cpf_gestao_limpo
        FROM tb_usuario s
        INNER JOIN (
          SELECT cpf_cnpj, id
          FROM dblink(
           'host=issec-live-db.c9aok84ka6e0.sa-east-1.rds.amazonaws.com dbname=accounts_api user=accounts_api password={SENHA_ACCOUNTS}',
              'SELECT cpf_cnpj, id FROM users'
          ) AS accounts(cpf_cnpj varchar(255), id uuid) 
        ) a ON s.sso_id = a.id 
        WHERE REGEXP_REPLACE(s.cpf_cnpj,'\D','', 'g') <> REGEXP_REPLACE(a.cpf_cnpj,'\D','', 'g')
    )
    SELECT * FROM divergencias;
    """

    try:
        conn = psycopg2.connect(**DB_GESTAO)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql_base)
        divergencias = cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"Erro crítico ao buscar divergências: {e}")
        return

    if not divergencias:
        print("Nenhuma divergência encontrada. Encerrando.")
        return

    # Coletar todos os CPFs únicos para as próximas consultas (Otimização)
    todos_cpfs = set()
    for d in divergencias:
        todos_cpfs.add(d['cpf_accounts_limpo'])
        todos_cpfs.add(d['cpf_gestao_limpo'])
    
    cpfs_tuple = tuple(todos_cpfs)

    # 2. VERIFICAR EXISTÊNCIA NO CONTRATO (SEGURADO)
    print(f"[2/4] Validando {len(todos_cpfs)} CPFs na tabela Segurado...")
    cpfs_existentes_segurado = set()
    
    try:
        conn = psycopg2.connect(**DB_CONTRATO)
        cur = conn.cursor()
        # Busca CPFs limpos da tabela segurado que coincidem com nossa lista
        sql_segurado = f"""
            SELECT REGEXP_REPLACE(cpf_cnpj, '\D','', 'g') 
            FROM segurado 
            WHERE REGEXP_REPLACE(cpf_cnpj, '\D','', 'g') IN %s
        """
        cur.execute(sql_segurado, (cpfs_tuple,))
        results = cur.fetchall()
        for row in results:
            cpfs_existentes_segurado.add(row[0]) # Adiciona ao Set de existência
        conn.close()
    except Exception as e:
        print(f"Erro ao consultar Segurado: {e}")
        return

    # 3. BUSCAR EMAILS (PESSOA/CONTATO)
    print("[3/4] Buscando e-mails no quarto banco...")
    mapa_emails = {} # { 'cpf_limpo': 'email' }
    
    try:
        conn = psycopg2.connect(**DB_PESSOA)
        cur = conn.cursor()
        
        # Query solicitada adaptada para buscar em lote
        # Precisamos buscar pelo CPF formatado ou limpo? 
        # Assumindo que o banco pessoa armazena formatado, aplicamos replace no WHERE
        sql_emails = f"""
            SELECT REGEXP_REPLACE(p.cpf_cnpj, '\D','', 'g') as cpf, c.valor as email
            FROM pessoa p 
            LEFT JOIN contato c ON p.id = c.pessoa_id
            WHERE c.tipo = 'EMAIL'
            AND REGEXP_REPLACE(p.cpf_cnpj, '\D','', 'g') IN %s
        """
        cur.execute(sql_emails, (cpfs_tuple,))
        results = cur.fetchall()
        
        for cpf, email in results:
            if email:
                mapa_emails[cpf] = email.strip() # Normaliza email
        conn.close()
    except Exception as e:
        print(f"Erro ao consultar Emails: {e}")
        return

    # 4. PROCESSAMENTO LÓGICO E CONTAGEM
    print("[4/4] Processando regras de negócio...")

    for item in divergencias:
        cpf_acc = item['cpf_accounts_limpo']
        cpf_ges = item['cpf_gestao_limpo']
        
        # Verifica existência na tabela segurado
        existe_acc = cpf_acc in cpfs_existentes_segurado
        existe_ges = cpf_ges in cpfs_existentes_segurado
        
        # Estrutura base do relatório
        linha_relatorio = {
            'id_accounts': item['id_account'],
            'sso_id_gestao': item['sso_id_gestao'],
            'cpf_gestao': item['cpf_visual_gestao'],
            'cpf_accounts': item['cpf_visual_accounts'],
            'existe_segurado_gestao': "SIM" if existe_ges else "NAO",
            'existe_segurado_accounts': "SIM" if existe_acc else "NAO",
            'dono_real_eh_accounts': "SIM" if existe_acc else "NAO", # Conforme solicitado
            'email_comum': None
        }

        # CASO 1: AMBOS INEXISTENTES EM SEGURADO
        if not existe_acc and not existe_ges:
            lista_ambos_inexistentes.append(linha_relatorio)
            continue

        # CASO 2: UM DELES INEXISTENTE
        if (existe_acc and not existe_ges) or (not existe_acc and existe_ges):
            lista_um_inexistente.append(linha_relatorio)
            # Nota: O fluxo pode parar aqui ou continuar para verificar email mesmo assim?
            # Pela lógica comum, se um não existe, é inconsistência de cadastro, mas vamos verificar email apenas se ambos existirem ou se solicitado.
            # Vou assumir que se falta um, já cai nessa categoria e encerra.
            continue 

        # CASO 3: AMBOS EXISTEM -> VERIFICAR EMAIL
        email_acc = mapa_emails.get(cpf_acc)
        email_ges = mapa_emails.get(cpf_ges)

        if email_acc and email_ges and (email_acc == email_ges):
            linha_relatorio['email_comum'] = email_acc
            lista_email_duplicado.append(linha_relatorio)
        else:
            # Se chegou aqui: existem no segurado, mas emails diferentes ou nulos
            lista_erros_outros.append(linha_relatorio)

    # 5. EXIBIÇÃO E SALVAMENTO
    print("\n" + "="*40)
    print("RESUMO FINAL DA OPERAÇÃO")
    print("="*40)
    print(f"1. E-mails Duplicados (Inconsistência Confirmada): {len(lista_email_duplicado)}")
    print(f"2. Um dos CPFs não existe em Segurado:             {len(lista_um_inexistente)}")
    print(f"3. Ambos CPFs não existem em Segurado:             {len(lista_ambos_inexistentes)}")
    print(f"4. Outros (Existem mas e-mail não bate/nulo):      {len(lista_erros_outros)}")
    print("-" * 40)
    print(f"TOTAL ANALISADO: {len(divergencias)}")
    print("="*40)

    # Headers para CSV
    headers = ['id_accounts', 'sso_id_gestao', 'cpf_gestao', 'cpf_accounts', 
               'existe_segurado_gestao', 'existe_segurado_accounts', 
               'dono_real_eh_accounts', 'email_comum']

    salvar_csv('relatorio_email_duplicado.csv', lista_email_duplicado, headers)
    salvar_csv('relatorio_um_cpf_inexistente.csv', lista_um_inexistente, headers)
    salvar_csv('relatorio_ambos_cpf_inexistentes.csv', lista_ambos_inexistentes, headers)
    salvar_csv('relatorio_outros_erros.csv', lista_erros_outros, headers)

if __name__ == "__main__":
    main()