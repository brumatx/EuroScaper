import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
import csv

class EuromilhoesDB:
    def __init__(self, db_path='instance/euromilhoes.db', csv_path='data/resultados.csv'):
        self.db_path = Path(db_path)
        self.csv_path = Path(csv_path)
        self.url = "https://www.jogossantacasa.pt/web/SCCartazResult/"
        
    def _ensure_instance_dir(self):
        """Garante que o diretório instance existe"""
        self.db_path.parent.mkdir(exist_ok=True)
        
    def _get_connection(self):
        """Retorna uma conexão com o banco de dados"""
        self._ensure_instance_dir()
        return sqlite3.connect(self.db_path)

    def view_database(self):
        """Visualiza os dados no banco"""
        try:
            if not self.db_path.exists():
                print("Banco de dados não encontrado!")
                return
                
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Verifica se a tabela existe
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='resultados';
            """)
            
            if not cursor.fetchone():
                print("Tabela 'resultados' não encontrada!")
                return
                
            df = pd.read_sql('SELECT * FROM resultados', conn)
            
            print("\n=== Informações do Banco de Dados ===")
            print(f"Total de registros: {len(df)}")
            print(f"\nPrimeiros 5 registros:")
            print(df.head())
            print(f"\nÚltimos 5 registros:")
            print(df.tail())
            
            conn.close()
            
        except Exception as e:
            print(f"Erro ao acessar banco: {str(e)}")

    def clear_database(self):
        """Apaga todos os dados do banco"""
        try:
            if not self.db_path.exists():
                print("Banco de dados não encontrado!")
                return
                
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS resultados")
            conn.commit()
            conn.close()
            
            print("Banco de dados limpo com sucesso!")
            
        except Exception as e:
            print(f"Erro ao limpar banco: {str(e)}")

    def delete_database(self):
        """Apaga o arquivo do banco de dados"""
        try:
            if self.db_path.exists():
                self.db_path.unlink()
                print("Arquivo do banco de dados removido com sucesso!")
            else:
                print("Arquivo do banco de dados não encontrado!")
                
        except Exception as e:
            print(f"Erro ao remover arquivo: {str(e)}")

    def import_from_csv(self):
        """Importa dados do CSV para o SQLite"""
        try:
            # Garantir que o diretório do CSV existe
            self.csv_path.parent.mkdir(exist_ok=True)
            
            if not self.csv_path.exists():
                print(f"Arquivo CSV não encontrado em: {self.csv_path}")
                return 0
                
            df = pd.read_csv(self.csv_path)
            df['DATE'] = pd.to_datetime(df['DATE']).dt.strftime('%Y-%m-%d')
            
            conn = self._get_connection()
            df.to_sql('resultados', conn, if_exists='replace', index=False)
            conn.close()
            
            return len(df)
            
        except Exception as e:
            print(f"Erro ao importar dados: {str(e)}")
            return 0

    def ler_ultimo_sorteio(self):
        """Lê o último sorteio registrado no arquivo CSV"""
        try:
            # Garantir que o diretório do CSV existe
            self.csv_path.parent.mkdir(exist_ok=True)
            
            with open(self.csv_path, mode='r') as file:
                reader = csv.reader(file)
                header = next(reader)  # Pula o cabeçalho
                ultimo_registro = None
                for row in reader:
                    if row:  # Ignora linhas vazias
                        ultimo_registro = row
                if ultimo_registro:
                    draw_number = int(ultimo_registro[0])
                    data_sorteio = datetime.strptime(ultimo_registro[1], "%Y-%m-%d").date()
                    return draw_number, data_sorteio, ultimo_registro
                return 0, None, None
                
        except FileNotFoundError:
            # Se o arquivo não existir, cria ele com o cabeçalho
            with open(self.csv_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['DRAW_NUMBER', 'DATE', 'MONTH', 'N1', 'N2', 'N3', 'N4', 'N5', 'S1', 'S2', 'WINNERS', 'TYPE'])
            return 0, None, None
            
        except Exception as e:
            print(f"Erro ao ler o CSV: {e}")
            return 0, None, None

    def salvar_novo_sorteio(self, dados):
        """Salva um novo sorteio no arquivo CSV"""
        try:
            # Garantir que o diretório do CSV existe
            self.csv_path.parent.mkdir(exist_ok=True)
            
            with open(self.csv_path, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(dados)
                print("Novo sorteio salvo no arquivo CSV!")
                print("Dados salvos:", dados)
                
        except Exception as e:
            print(f"Erro ao salvar no CSV: {e}")

    def obter_resultados(self):
        """Obtém os resultados do sorteio mais recente do site"""
        response = requests.get(self.url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # Obter a data e o número do sorteio
            data_info = soup.find("span", class_="dataInfo")
            if data_info:
                sorteio_texto = data_info.text.strip()
                sorteio_numero = int(sorteio_texto.split("Sorteio: ")[1].split("/")[0])
                data_sorteio = sorteio_texto.split("Data do Sorteio - ")[1]
                data_sorteio = datetime.strptime(data_sorteio, "%d/%m/%Y").date()
            else:
                print("Informação sobre o sorteio não encontrada.")
                return None

            # Obter os números e estrelas da segunda <li> que contém a ordem correta
            bet_middle = soup.find("div", class_="betMiddle twocol regPad")
            if bet_middle:
                resultados_ul = bet_middle.find("ul", class_="colums")
                if resultados_ul:
                    # Pegar a segunda <li>
                    todas_li = resultados_ul.find_all("li")
                    if len(todas_li) >= 2:
                        resultado_texto = todas_li[1].get_text(strip=True)
                        print("Texto do resultado original:", resultado_texto)
                        
                        # Separar números e estrelas mantendo a ordem
                        partes = resultado_texto.split("+")
                        if len(partes) == 2:
                            numeros = [num.strip() for num in partes[0].strip().split()]
                            estrelas = [star.strip() for star in partes[1].strip().split()]
                            print("Números extraídos:", numeros)
                            print("Estrelas extraídas:", estrelas)
                        else:
                            print("Formato do resultado não está correto")
                            return None
                    else:
                        print("Não foram encontrados elementos li suficientes")
                        return None
                else:
                    print("Lista de resultados não encontrada")
                    return None
            else:
                print("Div betMiddle não encontrada")
                return None

            # Obter o número de vencedores
            premio_ul = soup.find("ul", class_="colums")
            if premio_ul:
                vencedores_info = premio_ul.find_all("li", class_="litleCol")
                if vencedores_info:
                    vencedores = vencedores_info[0].text.strip()
                    numero_vencedores = int(vencedores) if vencedores.isdigit() else 0
                else:
                    numero_vencedores = 0
            else:
                numero_vencedores = 0

            # Determinar o tipo de sorteio
            dia_semana = data_sorteio.strftime("%A")
            sorteio_tipo = "Sexta-Feira" if dia_semana == "Friday" else "Terça-Feira"

            # Formatar a data
            data_sorteio_formatada = data_sorteio.strftime("%Y-%m")

            dados = [
                sorteio_numero,
                data_sorteio,
                data_sorteio_formatada,
                *numeros,
                *estrelas,
                numero_vencedores,
                sorteio_tipo
            ]
            
            print("Dados formatados para CSV:", dados)
            return dados
        else:
            print(f"Erro ao acessar o site. Código de status: {response.status_code}")
            return None

    def atualizar_resultados(self):
        """Atualiza o banco com os últimos resultados"""
        ultimo_sorteio, ultima_data, ultimo_registro = self.ler_ultimo_sorteio()
        novo_sorteio = self.obter_resultados()

        if novo_sorteio:
            print(f"Último sorteio registrado: {ultimo_sorteio}, Data: {ultima_data}")
            print(f"Sorteio mais recente obtido: {novo_sorteio[0]}, Data: {novo_sorteio[1]}")

            # Se não houver última data (CSV vazio) ou se a nova data for maior
            if ultima_data is None or novo_sorteio[1] > ultima_data:
                novo_sorteio[0] = ultimo_sorteio + 1  # Incrementar o número de sorteio em 1
                self.salvar_novo_sorteio(novo_sorteio)
                # Após salvar no CSV, importa para o banco
                self.import_from_csv()
            else:
                print("O sorteio mais recente já está registrado.")
                if ultimo_registro:
                    print("Último registro:", ultimo_registro)

def main():
    db = EuromilhoesDB()
    
    while True:
        print("\n=== Gerenciador do Banco de Dados ===")
        print("1. Visualizar dados")
        print("2. Limpar dados")
        print("3. Apagar banco de dados")
        print("4. Importar dados do CSV")
        print("5. Atualizar resultados")
        print("6. Sair")
        
        opcao = input("\nEscolha uma opção (1-6): ")
        
        if opcao == '1':
            db.view_database()
        elif opcao == '2':
            confirma = input("Tem certeza que deseja limpar os dados? (s/n): ")
            if confirma.lower() == 's':
                db.clear_database()
        elif opcao == '3':
            confirma = input("Tem certeza que deseja apagar o banco? (s/n): ")
            if confirma.lower() == 's':
                db.delete_database()
        elif opcao == '4':
            num_records = db.import_from_csv()
            print(f'Importados {num_records} registros com sucesso!')
        elif opcao == '5':
            db.atualizar_resultados()
        elif opcao == '6':
            print("Saindo...")
            break
        else:
            print("Opção inválida!")

if __name__ == '__main__':
    main()