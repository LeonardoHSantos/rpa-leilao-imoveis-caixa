import os
import json
import requests
from dotenv import load_dotenv
load_dotenv(override=True)

class BubbleAPI_Imovel:
    def __init__(self):
        self.api_key = os.getenv('BUBBLE_API_SECRET_KEY')
        self.base_URL = f'https://leilo-inteligente.bubbleapps.io/version-test/api/1.1/obj/Imovel'
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
                return True
        except Exception as e:
            print(f'Erro ao atualizar registro {unique_id}: {str(e)}')
            return None
    
    def create_record(self, data):
        try:
            response = requests.post(self.base_URL, headers=self.headers, data=json.dumps(data))
            if response.status_code == 201:
                return True
            else:
                print('Erro ao enviar dados:', response.text)
                return False
        except Exception as e:
            print(f'Erro ao inserir registro no banco de dados: {str(e)}')
            return None

    def consultar_imovel(self, cod_imovel):
        try:
            url = f'''{self.base_URL}?constraints=[{{
                "key": "cod_imovel",
                "constraint_type": "equals",
                "value": "{cod_imovel}"
            }}]'''
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                results = data.get("response", {}).get("results", [])
                if len(results):
                    return results[0].get("_id")  # Retorna o código do imóvel encontrado
                else:
                    print(f"Nenhum imóvel encontrado com cod_imovel {cod_imovel}.")
                    return None
            else:
                print(f"Erro ao consultar imóvel: {response.text}")
                return None
        except Exception as e:
            print(f'Erro ao consultar imóvel {cod_imovel}: {str(e)}')
            return False

    def bubble_api_imovel(self, data=dict):
        try:
            cod_imovel = data.get("cod_imovel")
            unique_id = self.consultar_imovel(cod_imovel=cod_imovel)
            if unique_id: # Registro do imóvel encontrado pelo cod_imovel
                return self.update_record(
                    data=data,
                    unique_id=unique_id
                    )
            else: # Será registrado o imóvel na tabela de imóveis
                return self.create_record(data=data)
        except Exception as e:
            print(f'Erro ao processar dados pela API Bubble: {str(e)}')
            return False


if __name__ == "__main__":

    data = {
            "value": "R$ 100.000,00",
            "cod_imovel": "1234",
            "item": "test-1234"
        }
    bubble_api = BubbleAPI_Imovel()
    response = bubble_api.bubble_api_imovel(data=data)
    print(response)
    print(response.text)