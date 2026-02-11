from NetworkSecurity.components.data_ingestion import DataIngestion
from NetworkSecurity.exception.exception import NetworkSecurityException
from NetworkSecurity.logging.logger import logging
import sys

from NetworkSecurity.entity.config_enitity import DataIngestionConfig
from NetworkSecurity.entity.config_enitity import TrainingPipelineConfig


if __name__=='__main__':
    try:
        trainingPiplineConfig=TrainingPipelineConfig()
        dataingestionconfig=DataIngestionConfig(trainingPiplineConfig)
        date_ingestion=DataIngestion(dataingestionconfig)
        logging.info("intiate the data ingestion ")
        dataingestionartificat=date_ingestion.initiate_data_ingestion()
        print(dataingestionartificat)
        
    except Exception as e:
           raise NetworkSecurityException(e,sys)