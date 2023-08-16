import pyodbc
from tkinter import messagebox

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
    except:
        messagebox.showerror("ATENÇÃO", "Erro ao conectar no banco")
    return conn, cursor

def close_db(): # Fecha a conexão com o banco de dados.
    global conn, cursor  # Utiliza as variáveis globais
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()

def update_db(): # Faz o update e zera as tentativas de login
    global cursor # Pega o cursor global

    try:
        query_update ="update USUAR set Qtd_TenLogInv = 0 where Qtd_TenLogInv = 5"

        cursor.execute(query_update) # Executa a query
        conn.commit() # Confirma o update

        rows_affected = cursor.rowcount # Verifica quantas linhas foram afetadas
        messagebox.showinfo("ATENÇÃO", f"Foram afetadas {rows_affected} linhas!")
        print(f"Foram afetadas {rows_affected} linhas!")
        return rows_affected # Retorna o valor da linha para armazenar em uma variavel


    except pyodbc.Error as e:
        messagebox.showerror("ATENÇÃO", f"Erro ao executar a consulta no banco de dados: {e}")

def run(): # Função para juntas todos os códigos.
    access_db()
    update_db()
    close_db()

run() # Roda o código
