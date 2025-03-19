import os
import json
import requests
from dotenv import load_dotenv
load_dotenv(override=True)


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
                response = requests.put(
                    url=url,
                    json=data,
                    headers=self.headers
                )
                if response.status_code == 204:
                    return True
                
                print(" 1-1 - Erro ao atualizar registro: ", response.status_code)
                print(" 1-2 - Erro: ", response.text)
                return False
        except Exception as e:
            print(f'Erro ao atualizar registro {unique_id}: {str(e)}')
            return None
    
    def create_record(self, data):
        try:
            response = requests.post(self.base_URL, headers=self.headers, data=json.dumps(data))
            if response.status_code == 201:
                print(">>>> Imóvel criado com sucesso.")
                return True
            
            print('Erro ao criar registro: ', response.text)
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