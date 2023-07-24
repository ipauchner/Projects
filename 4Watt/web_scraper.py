import os
from wx import CallAfter, Yield
from threading import Thread
import pandas as pd
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from pubsub import pub
from datetime import date, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep

import data_processing
import download_thread
import file_manager
from id import d_dic_inmet

class Scraper(Thread):
    def __init__(self, parent, app_data, call_after):
        Thread.__init__(self)
        self.main_frame = parent
        self.is_downloading = False
        self.call_after = call_after

        self.app_folder = os.path.join(Path.home(), '4waTT')
        self.historical_folder = os.path.join(self.app_folder, 'dados_historicos')
        self.temp_folder = os.path.join(self.app_folder, 'temp')

        self.app_data = app_data

        options = webdriver.ChromeOptions()
        options.headless = False
        options.add_argument("--window-size=1920x1080")
        options.add_experimental_option("prefs", {
            "download.default_directory": f"{self.temp_folder}",
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing_for_trusted_sources_enabled": False,
            "safebrowsing.enabled": False
        })

        WIN_DRIVER_PATH = os.path.join('drivers', 'chromedriver.exe')
        self.driver = webdriver.Chrome(executable_path=WIN_DRIVER_PATH, options=options)

        self.dp = data_processing.DataProcessing(app_data)
        self.file_manager = file_manager.Files(self.app_data)
        self.file_manager.check_folders()
        self.start()

    def run(self):
        self.download_dados_inmet()

    def download_dados_inmet(self):
        ''' Faz o download de todos os .zip da página `https://portal.inmet.gov.br/dadoshistoricos`. 
        Não baixa arquivos já existentes. '''

        self.is_downloading = True
        
        page_url = 'https://portal.inmet.gov.br/dadoshistoricos'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'}
        try:
            html_page = requests.get(page_url, headers=headers)
        except:
            raise Exception('Could not communicate with the host.')

        soup = BeautifulSoup(html_page.content, 'html.parser')        
        zips = soup.find_all('article', {'class': 'post-preview'})
        last_on_zip = self.file_manager.check_historial_data(zips)

        # Comunica o frame principal sobre as características deste download.
        self.main_frame.on_clear_progress()
        self.main_frame.overall_gauge.SetRange(len(zips))
        self.main_frame.overall_text.SetLabel('Baixando dados históricos...')

        zip_count = 0
        for zip in zips:
            url = zip.a['href']
            filename = os.path.basename(url)
            path = os.path.join(self.historical_folder, filename)
            self.main_frame.file_text.SetLabel(filename)

            # Se o arquivo não existe, vamos baixá-lo.
            if not os.path.isfile(path):
                self.main_frame.current_gauge.SetValue(0)

                try:
                    dl_thread = download_thread.DownloadThread(path, url)
                    dl_thread.join() # Apenas um download por vez. Tentar evitar block por IP.
                    CallAfter(pub.sendMessage, topicName='log', text=f"Arquivo {filename} baixado com sucesso.")
                except:
                    if os.path.isfile(path):
                        os.remove(path)
                    
                    CallAfter(pub.sendMessage, topicName='log', 
                    text=f'Erro ao baixar arquivo histórico {filename}. Abortando operação...', 
                    isError=True)
                    break

            zip_count += 1
            self.main_frame.overall_gauge.SetValue(zip_count)

        # Apenas no final, quando tivermos certeza que todos os arquivos foram baixados,
        # é seguro atualizar 'last_zip_date' no arquivo, se necessário.
        self.app_data['last_zip_date'] = last_on_zip
        pub.sendMessage('save-file')

        self.is_downloading = False
        pub.sendMessage('save-file')

        # Chama a função que deverá ser chamada após o download dos dados históricos.
        # Eu não posso dar .join() neste thread, pois vai bloquear a GUI. Se o usuário quer atualizar tudo,
        # tenho que dar um jeito de chamar essas funções aqui.

        if self.call_after:
            self.call_after()

        self.main_frame.on_clear_progress()
        pub.sendMessage('set-processing-being-done', value=False)

    def update_estacao(self, stations) -> list:
        ''' Esta função só deve ser chamada para estações que já estejam concatenadas.
        Acessa o site do INMET e baixa todos os dados faltantes desde a última entrada nos .csv
        salvos na pasta documentos até o dia anterior. O tempo de atualização não pode ser maior que 6 meses.
        Retorna uma lista com as estações que atualizaram com sucesso. '''

        CallAfter(pub.sendMessage, topicName='on_clear_progress')
        CallAfter(pub.sendMessage, topicName='update-overall-text', text="Atualizando arquivos...")

        csv_count = 1

        number_of_csvs = len(stations)
        CallAfter(pub.sendMessage, topicName='update-current-gauge-range', value=number_of_csvs)

        for csv in os.listdir(self.app_folder):
            estacao = csv
            station_key = estacao.split('.')[0] 
            if station_key not in stations:
                continue
            
            CallAfter(pub.sendMessage, topicName='update-file-text', text=f"Atualizando estação {estacao.split('.')[0]}...")
            Yield()

            # Lendo o arquivo já existente em disco.
            csv_path = os.path.join(self.app_folder, csv)
            df = pd.read_csv(csv_path, delimiter=',', dtype={'Chuva': object, 'Pressao': object, 
            'Radiacao': object, 'Temperatura': object, 'Umidade': object})

            # Descobrindo qual o último dia presente no csv e o dia anterior.
            ld = df['Data'].iloc[-1].split('-')
            last = date(int(ld[0]), int(ld[1]), int(ld[2]))
            last += timedelta(days=1)
            last_str = f"{last.month:02d}/{last.day:02d}/{last.year:04d}"
            yesterday = date.today() - timedelta(days=1)
            yesterday_str = f"{yesterday.month:02d}/{yesterday.day:02d}/{yesterday.year:04d}"

            # Se o intervalo entre a último data e o dia anterior for menor que dois dias,
            # não precisamos atualizar este .csv.
            if not yesterday - last >= timedelta(days=2):
                CallAfter(pub.sendMessage, topicName='log', text=f"O arquivo {csv} não precisou ser modificado, pois já está atualizado.")
                csv_count += 1
                CallAfter(pub.sendMessage, topicName='update-current-gauge', value=csv_count)
                continue

            # Faz download dos dados para atualizar.
            request = f"https://tempo.inmet.gov.br/TabelaEstacoes/{estacao.split('.')[0]}"
            inmet_df = None
            inmet_df = self.get_estacao_csv(request, last_str, yesterday_str)

            # TODO Adicionar maiores informacoes sobre o erro. O tempo de atualizacao e maior que 6 meses? A estacao nao existe mais e deixou de ser atualizada?
            if inmet_df is None:
                CallAfter(pub.sendMessage, topicName='log', text=f"Falha ao baixar dados da estação {estacao.split('.')[0]}.", isError=True)
                csv_count += 1
                CallAfter(pub.sendMessage, topicName='update-current-gauge', value=csv_count)
                stations.remove(station_key)
                continue

            drop_except = [x[0] for x in d_dic_inmet.values()]
            inmet_df = inmet_df.filter(drop_except)

            inmet_df = inmet_df.rename(columns={d_dic_inmet['Data'][0]: 'Data', d_dic_inmet['Hora'][0]: 'Hora', d_dic_inmet['Radiacao'][0]: 'Radiacao', 
            d_dic_inmet['Pressao'][0]: 'Pressao', d_dic_inmet['Temperatura'][0]: 'Temperatura', d_dic_inmet['Umidade'][0]: 'Umidade'})

            inmet_df['Data'] = inmet_df['Data'].apply(lambda x: self.dp.convert_date(x)) # Normalizando a data.
            inmet_df['Hora'] = inmet_df['Hora'].apply(lambda x: self.dp.convert_to_hour(x)) # Normalizando a hora.

            new_df = pd.concat([df, inmet_df], ignore_index=True)
            new_df.to_csv(csv_path, index=False)
            
            self.app_data['saved'][station_key]['last_updated'] = f"{yesterday.day}-{yesterday.month}-{yesterday.year}"

            csv_count += 1
            CallAfter(pub.sendMessage, topicName='log', text=f"Estação {estacao.split('.')[0]} atualizada com sucesso.")
            CallAfter(pub.sendMessage, topicName='update-current-gauge', value=csv_count)

    def get_estacao_csv(self, url: str, start: str, end: str) -> pd.DataFrame:
        ''' Usa selenium para solicitar dados de uma determinada estação em um certo período de tempo. '''

        try:
            self.driver.get(url)
            btn = self.driver.find_element(By.XPATH, '//*[@id="root"]/div[1]/div[1]/i')
            btn.click()

            inicio = WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="root"]/div[2]/div[1]/div[2]/div[4]/input')))
            fim =  WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="root"]/div[2]/div[1]/div[2]/div[5]/input')))
            getTableBtn =  WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="root"]/div[2]/div[1]/div[2]/button')))

            inicio.send_keys(start)
            fim.send_keys(end)
            getTableBtn.click()

            WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="root"]/div[2]/div[2]/div/div/div/span/a')))
            downloadBtn = self.driver.find_element(By.XPATH, '//*[@id="root"]/div[2]/div[2]/div/div/div/span/a')
            downloadBtn.click()
            sleep(1)
            
            return self.dp.process_inmet_file()
            
        except:
            return None
