from RPA_CAIXA.scrap import RPA_CAIXA


if __name__ == "__main__":
    RPA = RPA_CAIXA(base_url='https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis')
    RPA.scrap_caixa(UF='geral')
    # RPA.scrap_caixa(UF='TO', imovel_id="1555514708640", update_database=True)