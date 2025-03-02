from dotenv import load_dotenv
load_dotenv(override=True)

import os
import json
import requests
from bs4 import BeautifulSoup

from API_BUBBLE.api import BubbleAPI_Imovel

class RPA_LeilaoImovel:
    def __init__(self):
        self.rpa = None
        self.BASE_URL_SITE_LEILAO_IMOVEIS = os.getenv("BASE_URL_SITE_LEILAO_IMOVEIS")
        self.bubble_api = BubbleAPI_Imovel()

    def request_get_page(self, url: str):
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url=url, headers=headers)
            return response
        except Exception as e:
            error = f'Erro na requisição: {str(e)}'
            print(error)
            return None
    
    def scrap_pagination(self):
        data_links = {}
        limit_page = 50 # Definir um valor de 1 a 50, o site tem uma limitação de 50 páginas por consulta.
        for i in range(1, limit_page):
            print(f' ------------ PAGE: {i} ------------ ')
            data_links[i] = []
            # url = f'https://www.leilaoimovel.com.br/encontre-seu-imovel?s=&banco=1&pag={i}'
            url = f'https://www.leilaoimovel.com.br/encontre-seu-imovel?s=&banco=1&pagamento=3&pag={i}'
            response = self.request_get_page(url=url)

            if response:
                soup = BeautifulSoup(response.text, "html.parser")
                links = soup.select(".image .Link_Redirecter")
                for index, link in enumerate(links, start=0):
                    try:
                        url_page = f'{self.BASE_URL_SITE_LEILAO_IMOVEIS}{link.get("href")}'
                        data_links[i].append({
                            "item": index, "link_imovel": url_page
                        })
                    except Exception as e:
                        print(e)
            
        return self.scrap_page(data_links=data_links)
        
    def scrap_page(self, data_links: dict[list]):
        page_init = 37
        for index, page in data_links.items():
            
            if index >= page_init:
                for row in page:
                    item    = row["item"]
                    url     = row["link_imovel"]
                    response = self.request_get_page(url=url)
                    soup = BeautifulSoup(response.text, "html.parser")
                    data_links[index][item] = {}

                    value                   = self.filter_info_by_index(element=self.extract_information_by_css(soup=soup, path_css=".prices .row .value H2"), index=1)
                    documments_links        = self.extract_information_by_css(soup=soup, path_css=".sobre-imovel .documments a")
                    info_2                  = self.extract_information_by_css(soup=soup, path_css=".prices .row .info_2 p")
                    info_2_financiamento    = self.filter_info_by_index(element=info_2, index=0) 
                    info_2_parcelamento     = self.filter_info_by_index(element=info_2, index=1)
                    info_2_fgts             = self.filter_info_by_index(element=info_2, index=2)
                    # ----
                    payment                 = self.extract_information_by_css(soup=soup, path_css=".prices .row .payment p")
                    corretor_responsavel    = self.filter_info_by_index(element=payment, index=0)
                    # ---------------------------------------------------------------------------------
                    more                    = self.extract_information_by_css(soup=soup, path_css=".sobre-imovel .more div")

                    data_links[index][item]["link_imovel"]              = url
                    data_links[index][item]["value"]                    = value
                    data_links[index][item]["info_2_financiamento"]     = info_2_financiamento
                    data_links[index][item]["info_2_parcelamento"]      = info_2_parcelamento
                    data_links[index][item]["info_2_fgts"]              = info_2_fgts
                    data_links[index][item]["corretor_responsavel"]     = self.remove_word_spacing(content=corretor_responsavel)
                    data_links[index][item].update(self.extract_more(contents=more))
                    data_links[index][item].update(self.extract_documments_links(contents=documments_links))
                    # ------------------------------------------------------------------------------------------------------
                    data_links[index][item]["bubble_api_imovel"] = self.bubble_api.bubble_api_imovel(data=data_links[index][item])

                    with open("DATA/data_links.json", "w", encoding="utf-8") as f:
                        json.dump(data_links, f, indent=4, ensure_ascii=False)
            
        return
    
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
    
    def get_data_by_index(self, contents, index, attr):
        try:
            return contents[index].get(attr)
        except:
            return None
        
    def remove_word_spacing(self, content : str):
        try:
            content = " ".join(list(map(lambda x: x.strip(), content.split("\n"))))
            return content
        except:
            return None
    
    def data_split(self, data: str, target: str, index: int):
        try:
            return data.split(target)[index].strip()
        except:
            return None
    
    def extract_more(self, contents):
        try:
            data_more = {
                "localizacao": None,
                "tipo": None,
                "banco": None,
                "cod_imovel": None,
                "data_inclusao": None,
                "matricula": None,
                "comarca": None,
                "oficio": None,
                "incricao_imobiliaria": None,
                "averbacao_dos_leiloes_negativos": None,
                "descricao": None,
            }
            for content in contents:
                data = self.remove_word_spacing(content=content.text)
                if "Localização:" in data:
                    data_more["localizacao"] = self.data_split(data=data, target="Localização:", index=1)
                elif "Tipo:" in data:
                    data_more["tipo"] = self.data_split(data=data, target="Tipo:", index=1)
                elif "Banco:" in data:
                    data_more["banco"] = self.data_split(data=data, target="Banco:", index=1)
                elif "Código Imóvel:" in data:
                    data_more["cod_imovel"] = self.data_split(data=data, target="Código Imóvel:", index=1)
                elif "Data de Inclusão:" in data:
                    data_more["data_inclusao"] = self.data_split(data=data, target="Data de Inclusão:", index=1)
                elif "Matrícula:" in data:
                    data_more["matricula"] = self.data_split(data=data, target="Matrícula:", index=1)
                elif "Comarca:" in data:
                    data_more["comarca"] = self.data_split(data=data, target="Comarca:", index=1)
                elif "Ofício:" in data:
                    data_more["oficio"] = self.data_split(data=data, target="Ofício:", index=1)
                elif "Inscrição imobiliária:" in data:
                    data_more["incricao_imobiliaria"] = self.data_split(data=data, target="Inscrição imobiliária:", index=1)
                elif "Averbação dos leilões negativos:" in data:
                    data_more["averbacao_dos_leiloes_negativos"] = self.data_split(data=data, target="Averbação dos leilões negativos:", index=1)
                elif "Descrição:" in data:
                    data_more["descricao"] = self.data_split(data=data, target="Descrição:", index=1)

            return data_more
        
        except Exception as e:
            print(f'Erro ao extrair dados: {str(e)}')
            return None

    def extract_documments_links(self, contents):
        data = {
            "metricula": None,
            "edital": None,
        }
        data["edital"]      = self.get_data_by_index(contents=contents, index=0, attr="href")
        data["metricula"]   = self.get_data_by_index(contents=contents, index=1, attr="href")
        return data

    