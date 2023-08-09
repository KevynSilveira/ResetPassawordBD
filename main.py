import pyodbc
import os

conn = None  # Variável global para armazenar a conexão com o banco de dados
cursor = None  # Variável global para armazenar o cursor

def access_db(): # Acessa o banco de dados
    global conn, cursor  # Utiliza as variáveis globais

    try:
        # Credenciais do banco de dados
        server = ""
        database = ""
        username = ""
        password = ""

        # Monta os dados para enviar para o banco de dados (credenciais)
        conn_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        print("Conexão concluida!")
    except:
        print("Erro ao conectar no banco")
    return conn, cursor

def close_db(): # Fecha a conexão com o banco de dados.
    global conn, cursor  # Utiliza as variáveis globais
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()

def update_db(cliente, agente, novo_agente, estabelecimento, parcela, data_inicio, data_fim, ordena): # Faz o update do novo agente cobrador
    global cursor # Pega o cursor global

    try:
        query_cte = f"DECLARE @cliente INT = {cliente}; " \
                    f"DECLARE @filial INT = (SELECT cgc_matriz FROM CLIEN WHERE codigo = @cliente); " \
                    f"DECLARE @agente INT = {agente}; " \
                    f"DECLARE @estabelecimento INT = {estabelecimento}; " \
                    f"DECLARE @datainicio DATE = '{data_inicio}'; " \
                    f"DECLARE @datafim DATE = '{data_fim}'; " \
                    f"DECLARE @parcela varchar(255) = '{parcela}'; " \
                    f"WITH CTE AS ( " \
                    f" SELECT TOP 800" \
                    f"  Cod_Documento AS Código," \
                    f"  Num_Documento AS Documento," \
                    f"  dat_vencimento AS Vencimento," \
                    f"  Par_Documento AS Parcela," \
                    f"  Cod_Agente AS Ag_Cobrador," \
                    f"  Cod_Cliente AS Cliente," \
                    f"  FORMAT(Vlr_Documento, 'C') AS Preco" \
                    f" FROM CTREC" \
                    f" WHERE cod_estabe = @estabelecimento" \
                    f"  AND cod_agente = @agente" \
                    f"  AND status = 'A'" \
                    f"  AND Par_Documento = @parcela" \
                    f"  AND dat_vencimento BETWEEN @datainicio AND @datafim" \
                    f"  AND (cod_cliente = @cliente OR cgc_matriz = @filial)" \
                    f" ORDER BY {ordena}" \
                    f") "

        query_update = f"UPDATE CTE SET Ag_Cobrador = {novo_agente};"
        final_query = query_cte + query_update # Concatena as query

        cursor.execute(final_query) # Executa a query
        conn.commit() # Confirma o update

        rows_affected = cursor.rowcount # Verifica quantas linhas foram afetadas
        return rows_affected # Retorna o valor da linha para armazenar em uma variavel

    except pyodbc.Error as e:
        print("Erro ao executar a consulta no banco de dados:", e)