from RPA_CAIXA.scrap import RPA_CAIXA

RPA = RPA_CAIXA(base_url='https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis')
RPA.scrap_caixa(UF='geral')
# RPA.scrap_caixa(UF='AL')