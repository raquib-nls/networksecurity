from NetworkSecurity.components.data_ingestion import DataIngestion
from NetworkSecurity.exception.exception import NetworkSecurityException
from NetworkSecurity.logging.logger import logging
import sys

from NetworkSecurity.components.data_validation import DataValidation
from NetworkSecurity.entity.config_enitity import DataIngestionConfig,DataValidationConfig
from NetworkSecurity.entity.config_enitity import TrainingPipelineConfig


if __name__=='__main__':
    try:
        trainingPiplineConfig=TrainingPipelineConfig()
        dataingestionconfig=DataIngestionConfig(trainingPiplineConfig)
        date_ingestion=DataIngestion(dataingestionconfig)
        logging.info("intiate the data ingestion ")
        dataingestionartificat=date_ingestion.initiate_data_ingestion()
        logging.info("Data initiation completed")
        print(dataingestionartificat)

        data_validation_config=DataValidationConfig(trainingPiplineConfig)
        data_validation=DataValidation(dataingestionartificat,data_validation_config)
        logging.info("initiate the data validation")
        data_validation_artifcat=data_validation.initiat_data_validation()
        logging.info("data validation completed")
        print(data_validation_artifcat)

        
    except Exception as e:
           raise NetworkSecurityException(e,sys)