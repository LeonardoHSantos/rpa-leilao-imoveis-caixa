import os
import re
import json
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup

from dotenv import load_dotenv
load_dotenv(override=True)

class RPA_CAIXA:
    def __init__(self, base_url):
        self.base_url = base_url
        self.data_fields_check = ["Tipo de imóvel", "Garagem", "Número do imóvel", "Matrícula", "Comarca", "Ofício", "Inscrição imobiliária", "Averbação dos leilões negativos", "Quartos"]
        self.estados_completos = {
            "AC": "ACRE",
            "AL": "ALAGOAS",
            "AM": "AMAZONAS",
            "AP": "AMAPÁ",
            "BA": "BAHIA", 
            "CE": "CEARÁ",
            "DF": "DISTRITO FEDERAL",
            "ES": "ESPÍRITO SANTO",
            "GO": "GOIÁS", 
            "MA": "MARANHÃO",
            "MG": "MINAS GERAIS",
            "MS": "MATO GROSSO DO SUL",
            "MT": "MATO GROSSO", 
            "PA": "PARÁ",
            "PB": "PARAÍBA",
            "PE": "PERNAMBUCO",
            "PI": "PIAUÍ",
            "PR": "PARANÁ", 
            "RJ": "RIO DE JANEIRO",
            "RN": "RIO GRANDE DO NORTE",
            "RO": "RONDÔNIA",
            "RR": "RORAIMA", 
            "RS": "RIO GRANDE DO SUL",
            "SC": "SANTA CATARINA",
            "SE": "SERGIPE",
            "SP": "SÃO PAULO", 
            "TO": "TOCANTINS"
        }
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
        ]
        self.bubble_api = BubbleAPI_Imovel()

    def prepare_headers(self):
        return {
            "User-Agent": random.choice(self.user_agents)
        }
    
    def scrap_caixa(self, UF):
        url = f'https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{UF}.csv'
        headers = self.prepare_headers()
        response = requests.get(
            url=url,
            headers=headers
            )
        if response.status_code == 200:
            linhas = response.text.strip().split('\n')[1:]
            linhas.pop(1)
            rows_df = list(map( lambda x: x.split(";"), linhas))
            columns = list(map( lambda x: x.strip(), rows_df[0]))
            df_imoveis = pd.DataFrame(columns=columns, data=rows_df[1:])
            print(df_imoveis)
            print(df_imoveis.info())
            self.preparar_payload(df_imoveis=df_imoveis)
            return
    
    def preparar_payload(self, df_imoveis: pd.DataFrame):
        payload = {}
        for i in df_imoveis.index:
            
            # if "8444404014020" in df_imoveis["Link de acesso"][i]:
            payload[i] = {}
            data = {}
            for col in df_imoveis.columns:
                data.update({
                    col: str(df_imoveis[col][i]).strip()
                })

            # try:
            data = self.delete_column_payload(payload=data, colname="Descrição")
            _UF = data["UF"]
            data["Estado"] = self.estados_completos.get(_UF, "")
            _TOTAL_UF = len(df_imoveis["UF"])
            numero_matricula = data.get("N° do imóvel") or data.get("NḞ do imóvel") or None
            if numero_matricula:
                data["N° do imóvel"] = numero_matricula
                print(f"\n ----------- ID: {i} | UF: {_UF} - QTD: {_TOTAL_UF} | numero_matricula: {numero_matricula}")
                # ----------------------------------------------------------------------------------------------------------------------
                # ----------------------------------------------------------------------------------------------------------------------
                try:
                    del data["NḞ do imóvel"]
                except:
                    pass
                # ----
                valor_avaliacao = data.get("Valor de avaliação") or data.get("Valor de avaliaçăo")
                data["Valor de avaliação"] = valor_avaliacao
                try:
                    del data["Valor de avaliaçăo"]
                except:
                    pass
                # ----------------------------------------------------------------------------------------------------------------------
                if len(numero_matricula) < 13:
                    total_zeros = 13 - len(numero_matricula)
                    nova_string = []
                    for i in range(total_zeros):
                        nova_string.append("0")
                    numero_matricula ="".join(nova_string) + numero_matricula

                data["link_pdf_maticula"] = f'https://venda-imoveis.caixa.gov.br/editais/matricula/{_UF}/{numero_matricula}.pdf'
                # ----------------------------------------------------------------------------------------------------------------------

                link_page = data["Link de acesso"]
                valor_avaliacao = data["Valor de avaliação"]
                data["info"] = self.scrap_page(link_imovel=link_page, valor_avaliacao=valor_avaliacao)
                data = self.flatten_dict(d=data)

                # ----------
                data = self.delete_column_payload(payload=data, colname="UF")
                data = self.delete_column_payload(payload=data, colname="rua")
                data = self.rename_column_payload(payload=data, old_name="Endereço", new_name="rua")
                data = self.rename_column_payload(payload=data, old_name="N° do imóvel", new_name="imovel_id")
                
                status_bubble_api = False
                if data:
                    status_bubble_api = self.bubble_api.bubble_api_imovel(data=data)
                    data["status_bubble_api"] = status_bubble_api
                    print(f">>>> status_bubble_api: {status_bubble_api}")
                    print(json.dumps(data, indent=4))
                payload[i] = data

                with open("data_links_v2.json", "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=4, ensure_ascii=False)

                # return

    def scrap_page(self, link_imovel, valor_avaliacao):
        try:
            response = requests.get(
                url=link_imovel,
                headers=self.prepare_headers(),
            )
            soup = BeautifulSoup(response.text, "html.parser")
            data = self.filter_info_by_fields( elements=self.extract_information_by_css(soup=soup, path_css=".control-item p span") )
            endereco =  self.extract_information_by_index(self.filter_info_by_index( element = self.extract_information_by_css( soup=soup, path_css=".related-box p" ), index = 0 ), target="Endereço:")
            data["pagamentos"] =  self.extract_text( self.filter_info_by_index( element = self.extract_information_by_css( soup=soup, path_css=".related-box p" ), index = 2 ) )
            
            data["Descrição"] =  self.extract_information_by_index(self.filter_info_by_index( element = self.extract_information_by_css( soup=soup, path_css=".related-box p" ), index = 1 ), target="Descrição:")
            # -----------------------------------------------------------------------------
            
            data["rua"] = endereco
            data["cep"] = self.extract_CEP(endereco=endereco)

            # -----------------------------------------------------------------------------
            titulo = self.extract_title(soup=soup)
            if titulo:
                data["titulo"] = titulo

            # -----------------------------------------------------------------------------
            link_edital = self.extract_link_edital(soup=soup)
            if link_edital:
                data["link_edital"] = link_edital
            # -----------------------------------------------------------------------------
            link_leiloeiro = self.extract_link_leiloeiro(soup=soup)
            if link_leiloeiro:
                data["link_leiloeiro"] = link_leiloeiro
            # -----------------------------------------------------------------------------
            info_leilao = self.extract_info_leilao(soup=soup)
            if info_leilao:
                data["info_leilao"] = info_leilao
            # -----------------------------------------------------------------------------
            info_valores_leilao = self.extract_valores_leiloes(soup=soup, valor_avaliacao=valor_avaliacao)
            if info_valores_leilao:
                data["info_valores_leilao"] = info_valores_leilao
            # -----------------------------------------------------------------------------
            info_valores_metragem = self.extract_valores_areas_privativa_e_terreno(soup=soup)
            if info_valores_metragem:
                data["valores_metragem"] = info_valores_metragem

            # -----------------------------------------------------------------------------
            info_images = self.extract_images(soup=soup)
            if info_valores_metragem:
                data["images"] = info_images

            # -----------------------------------------------------------------------------
            info_tempoRestanteVenda = self.extract_tempo_restante(soup=soup)
            if info_tempoRestanteVenda:
                data["previsao_encerramento_venda"] = info_tempoRestanteVenda

            # -----------------------------------------------------------------------------

            # data["precos"] =  self.extract_precos( self.filter_info_by_index( element = self.extract_information_by_css( soup=soup, path_css=".content p" ), index = 0 ) )
            return data
        except:
            return None
    
    def extract_tempo_restante(self, soup):
        try:
            tempoRestanteVenda = "-"
            # Expressão regular para capturar datas dentro do padrão desejado
            date_pattern = re.compile(r'\+\s*"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})"\s*\+\s*"@@"\s*\+')

            # Lista para armazenar as datas encontradas
            found_dates = []

            # Procurando dentro das tags <script> no HTML
            scripts = soup.find_all('script')

            for script in scripts:
                if script.string:  # Garantir que o script tenha conteúdo
                    matches = date_pattern.findall(script.string)
                    found_dates.extend(matches)

            # Exibindo os resultados encontrados
            if found_dates:
                for date in found_dates:
                    tempoRestanteVenda = date
            
            return tempoRestanteVenda
        except:
            return "-"
    
    def extract_title(self, soup):
        try:
            return soup.select('.content-section .control-item h5')[0].text.strip()
        except:
            return None
    
    def extract_images(self, soup):
        try:
            data = {}
            images = soup.select('.thumbnails img')
            if images:
                limit_images = 5
                for index, img in enumerate(images, start=1):
                    if index <= limit_images:
                        src = img.get('src') 
                        if src:
                            data[f'imagem_{index}'] = f'https://venda-imoveis.caixa.gov.br/{src}'
            return data
        except:
            return None
        
    def extract_valores_areas_privativa_e_terreno(self, soup):
        try:
            data = {}
            link = soup.select('.content .control-item')[1]
            if link:
                if "Área privativa =" in link.text and "Área do terreno =" in link.text:
                    values = link.text.split("Área privativa")[1].split("Área do terreno")
                    data["area_privativa"] = values[0].replace("=", "").strip()
                    data["area_terreno"] = values[1].replace("=", "").strip()
                elif "Área do terreno" in link.text:
                    values = link.text.split("Área do terreno")
                    data["area_terreno"]  = values[1].replace("=", "").strip()
            return data
        except:
            return None
        
    def extract_valores_leiloes(self, soup, valor_avaliacao):
        try:
            data = {}
            info_leilao = soup.select('.content p')[0].get_text()
            valores = re.findall(r'R\$\s([\d\.]+,\d{2})', info_leilao)
            if "Valor mínimo de venda 1º Leilão" in info_leilao:
                data["Valor mínimo de venda 1º Leilão"] = valores[1]
                data["Valor desconto de venda 1º Leilão"] = self.calc_desconto(valor_avaliacao=valor_avaliacao, valor_leilao=valores[1])

            if "Valor mínimo de venda 2º Leilão" in info_leilao:
                data["Valor mínimo de venda 2º Leilão"] = valores[2]
                data["Valor desconto de venda 2º Leilão"] = self.calc_desconto(valor_avaliacao=valor_avaliacao, valor_leilao=valores[2])
            
            return data
        except:
            return None
    
    def calc_desconto(self, valor_avaliacao: str, valor_leilao: str):
        try:
            valor_avaliacao = float(valor_avaliacao.replace(".", "").replace(",", "."))
            valor_leilao = float(valor_leilao.replace(".", "").replace(",", "."))
            valor_desc = round(100 - ((valor_leilao / valor_avaliacao) * 100), 2)
            if valor_desc > 0:
                return f'{valor_desc:.2f}'
            return "0.00"
        except:
            return None
        
    def extract_link_edital(self, soup):
        try:
            link_edital = None
            link = soup.select('.form-set a')[0]  # Seleciona o primeiro <a> dentro da classe 'form-set'
            if link:
                # Extraindo o valor do atributo 'onclick'
                onclick_value = link.get('onclick')  # Pega o valor do atributo 'onclick'
                
                if onclick_value:
                    # Extraindo a URL do atributo onclick usando expressão regular
                    match = re.search(r"ExibeDoc\('([^']+)'\)", onclick_value)
                    if match:
                        # A URL que você deseja está no primeiro grupo da correspondência
                        pdf_url = match.group(1)
                        link_edital = f'https://venda-imoveis.caixa.gov.br{pdf_url}'

            return link_edital
        except:
            return None
    
    def extract_link_leiloeiro(self, soup):
        try:
            link_leiloeiro = None
            link = soup.select('.form-set button')[0]
            if link:
                onclick_value = link.get('onclick') 
                if onclick_value:
                    match = re.search(r"SiteLeiloeiro\(\"([^\"]+)\"\)", onclick_value)
                    if match:
                        link_leiloeiro = match.group(1)

            return link_leiloeiro
        except:
            return None
    
    def extract_CEP(self, endereco):
        try:
            cep = endereco.split("CEP:")[1].split(",")[0].strip()
            return cep
        except:
            return None
    
    def extract_info_leilao(self, soup):
        data = {}
        try:
            info_leilao = soup.select('.related-box span')
            if info_leilao:
                for info in info_leilao:
                    if "Edital:" in info.text:
                        print(info.text.strip())
                        data["Edital"] = str(info.text).split(":")[-1].strip()
                    
                    if "Número do item:" in info.text:
                        print(info.text.strip())
                        data["Número do item"] = str(info.text).split(":")[-1].strip()
                    
                    if "Leiloeiro(a):" in info.text:
                        print(info.text.strip())
                        data["Leiloeiro(a)"] = str(info.text).split(":")[-1].strip()
                    
                    if "Data do" in info.text:
                        print(info.text.strip())
                        data_leilao = str(info.text).split(" - ")
                        numero_leilao = data_leilao[0].split("Data do")[-1].strip()
                        data[f"Data do {numero_leilao}"] = data_leilao[1].strip()
                        data[f"Hora do {numero_leilao}"] = data_leilao[2].strip()
                    
            return data
        except:
            return None

    def flatten_dict(self, d, parent_key='', sep='_'):
        try:
            items = []
            for k, v in d.items():
                try:
                    new_key = f"{parent_key}{sep}{k}" if parent_key else k
                    new_key = new_key.replace("data_", "").replace("info_", "").replace("precos_", "").replace("pagamentos_", "").replace("leilao_", "").replace("valores_", "").replace("metragem_", "").replace("images_", "")
                    new_key = str(new_key).strip()
                    if isinstance(v, dict):  # Se o valor for um dicionário, recursão
                        items.extend(self.flatten_dict(v, new_key, sep=sep).items())
                    else:
                        items.append((new_key, v))
                except:
                    pass

            return dict(items)
        except:
            return None
        
    def delete_column_payload(self, payload, colname):
        try:
            del payload[colname]
            return payload
        except:
            return payload
    
    def rename_column_payload(self, payload, old_name, new_name):
        try:
            payload[new_name] = payload[old_name]
            del payload[old_name]
            return payload
        except:
            return payload
    
    def extract_information_by_css(self, soup, path_css):
        try:
            content = soup.select(path_css)
            return content
        except:
            return None
        
    def filter_info_by_index(self, element, index):
        try:
            return element[index].text.strip()
        except:
            return None
        
    def extract_information_by_index(self, value:str, target: str):
        try:
            return value.split(target)[-1].strip()
        except:
            return None
    
    def extract_text(self, element):
        data_item = {
            "aceita_FGTS": "Sim",
            "aceita_FINANCIAMENTO_HABT": "Sim",
            "aceita_PARCELAMENTO": "Sim",
            "aceita_CONSORCIO": "Sim",
            # ----------------------------
            "desepesas_e_tributos_pagas_pelo_comprador": "Não",
            "desepesas_de_condominio_pelo_comprador": "Não",
        }
        try:
            if "Imóvel NÃO aceita utilização de FGTS" in element:
                data_item["aceita_FGTS"] = "Não"
            if "Imóvel NÃO aceita financiamento habitacional" in element:
                data_item["aceita_FINANCIAMENTO_HABT"] = "Não"
            if "Imóvel NÃO aceita parcelamento" in element:
                data_item["aceita_PARCELAMENTO"] = "Não"
            if "Imóvel NÃO aceita consórcio" in element:
                data_item["aceita_CONSORCIO"] = "Não"
            if "O pagamento das despesas de tributos incidentes sobre o imóvel ficará a cargo do comprador" in element:
                data_item["desepesas_e_tributos_pagas_pelo_comprador"] = "Sim"
            if "As despesas de tributos, até a data da venda" in element:
                data_item["desepesas_e_tributos_pagas_pelo_comprador"] = "Sim"
            if "O pagamento das despesas de condomínio também ficará a cargo do comprador" in element:
                data_item["desepesas_de_condominio_pelo_comprador"] = "Sim"
            return data_item
        except:
            return None
    
    def extract_precos(self, element):
        try:
            data = element.split("(")[0].split("Valor")
            data_item = {
                "valor_avaliacao": data[1].split("R$")[-1].strip(),
                "valor_minimo_venda": data[2].split("R$")[-1].strip(),
            }
            return data_item
        except:
            return None
    
    def filter_info_by_fields(self, elements):
        try:
            data = {}
            for el in elements:
                # print(el.text)
                for index, field in enumerate(self.data_fields_check, start=0):
                    text = str(el.text).strip()
                    if field in el.text:
                        lista_imovel = text.split(":")
                        lista_imovel_limpa = [item.strip() for item in lista_imovel]
                        if lista_imovel_limpa[-1]:
                            data[field] = lista_imovel_limpa[-1]

            return data
        except:
            return None
        


class BubbleAPI_Imovel:
    def __init__(self):
        self.api_key = os.getenv('BUBBLE_API_SECRET_KEY')
        self.base_URL = f'https://leilo-inteligente.bubbleapps.io/version-test/api/1.1/obj/ImoveisCaixa'
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
            }
    
    def gel_all_data(self):
        try:
            response = requests.get(self.base_URL, headers=self.headers)
            results = response.json()["response"]["results"]
            return results
        except Exception as e:
            print(f'Erro ao obter dados data tabela de imóveis: {str(e)}')
            return None
    
    def update_record(self, data, unique_id):
        try:
            if unique_id:
                url = f'{self.base_URL}/{unique_id}'
                requests.put(
                    url=url,
                    json=data,
                    headers=self.headers
                )
                print(">>>> Imóvel atualizado com sucesso.")
                return True
        except Exception as e:
            print(f'Erro ao atualizar registro {unique_id}: {str(e)}')
            return None
    
    def create_record(self, data):
        try:
            response = requests.post(self.base_URL, headers=self.headers, data=json.dumps(data))
            if response.status_code == 201:
                print(">>>> Imóvel criado com sucesso.")
                return True
            else:
                print('Erro ao enviar dados:', response.text)
                return False
        except Exception as e:
            print(f'Erro ao inserir registro no banco de dados: {str(e)}')
            return None

    def consultar_imovel(self, imovel_id):
        try:
            
            url = f'{self.base_URL}?constraints=[{{"key": "imovel_id", "constraint_type": "equals", "value": "{imovel_id}"}}]'
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                results = data.get("response", {}).get("results", [])
                if len(results):
                    return results[0].get("_id")  # Retorna o código do imóvel encontrado
                else:
                    # print(f"Nenhum imóvel encontrado com imovel_id {imovel_id}.")
                    return None
            else:
                print(f"Erro ao consultar imóvel: {response.text}")
                return None
        except Exception as e:
            print(f'Erro ao consultar imóvel {imovel_id}: {str(e)}')
            return False

    def bubble_api_imovel(self, data=dict):
        try:
            imovel_id = data.get("imovel_id")
            unique_id = self.consultar_imovel(imovel_id=imovel_id)
            if unique_id: # Registro do imóvel encontrado pelo imovel_id
                return self.update_record(
                    data=data,
                    unique_id=unique_id
                    )
            else: # Será registrado o imóvel na tabela de imóveis
                return self.create_record(data=data)
        except Exception as e:
            print(f'Erro ao processar dados pela API Bubble: {str(e)}')
            return False