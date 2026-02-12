from NetworkSecurity.entity.artifact_entity import DataIngestionArtifact
from NetworkSecurity.entity.artifact_entity import DataValidationArtifact
from NetworkSecurity.entity.config_enitity import DataValidationConfig
from NetworkSecurity.exception.exception import NetworkSecurityException
from NetworkSecurity.logging.logger import logging
from NetworkSecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from NetworkSecurity.utils.main_utils.utils import read_yaml_file
from NetworkSecurity.utils.main_utils.utils import write_yaml_file
from scipy.stats import ks_2samp 
import pandas as pd
import os,sys

class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,data_validation_config:DataValidationConfig):
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self._schema_config=read_yaml_file(SCHEMA_FILE_PATH)

        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def validate_number_of_columns(self,dataframe:pd.DataFrame)->bool:
        try:
            number_of_columns=len(self._schema_config)
            logging.info(f"Required number of columns:{number_of_columns}")
            logging.info(f"Data frame has columns:{len(dataframe.columns)}")
            if len(dataframe.columns)==number_of_columns:
                return True
            
            return False
        

        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def validate_numerical_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            status = True
            schema_columns = self._schema_config["columns"]

            for column, dtype in schema_columns.items():

                if column not in dataframe.columns:
                    logging.error(f"{column} missing in dataframe")
                    status = False
                    continue

                # check numeric columns
                if dtype in ["int", "float"]:
                    if not pd.api.types.is_numeric_dtype(dataframe[column]):
                        logging.error(f"{column} should be numeric but found {dataframe[column].dtype}")
                        status = False

            return status

        except Exception as e:
            raise NetworkSecurityException(e, sys)



    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e,sys)

        
    def detect_detectio_drift(self,base_df,current_df,threshold=0.05)->bool:
        try:
            status=True
            report={}
            for column in base_df.columns:
                # if non num clmns appear than skip
                if not pd.api.types.is_numeric_dtype(base_df[column]):
                    continue

                d1=base_df[column].dropna()
                d2=current_df[column].dropna()
                
                if len(d1) == 0 or len(d2) == 0:
                    continue
                is_same_dist=ks_2samp(d1,d2)
                if threshold <=is_same_dist.pvalue:
                    is_found=False
                else:
                    is_found=True
                    status=False
                report.update({column:{
                        "p_value":float(is_same_dist.pvalue),
                        "drfit_status":is_found
                    }})
            drift_report_file_path=self.data_validation_config.drift_report_file_path
            dir_path=os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path,exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path,content=report)
            return status

        except Exception as e:
            raise NetworkSecurityException(e,sys)


    def initiat_data_validation(self)->DataValidationArtifact:
        try:

            error_message=""

            train_file_path=self.data_ingestion_artifact.trained_file_path
            test_file_path=self.data_ingestion_artifact.tested_file_path

            # read data from train and test
            train_dataFrame=DataValidation.read_data(train_file_path)
            test_dataFrame=DataValidation.read_data(test_file_path)

            # validate no. of clmns
            status=self.validate_number_of_columns(dataframe=train_dataFrame)
            if not status:
                error_message=f"{error_message} Train dataframe doesn't contain all coloumns\n"
            status=self.validate_number_of_columns(dataframe=test_dataFrame)
            if not status:
                error_message=f"{error_message} Test dataframe doesn't contain all coloumns\n"

            # validate numeric columns
            status = self.validate_numerical_columns(train_dataFrame)
            if not status:
                error_message += "Train dataframe numeric column validation failed\n"

            status = self.validate_numerical_columns(test_dataFrame)
            if not status:
                error_message += "Test dataframe numeric column validation failed\n"

            
            # check data drift
            status=self.detect_detectio_drift(base_df=train_dataFrame,current_df=test_dataFrame)
            dir_path=os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path,exist_ok=True)

            train_dataFrame.to_csv(
                self.data_validation_config.valid_train_file_path,index=False,header=True
            )
            test_dataFrame.to_csv(
                self.data_validation_config.valid_test_file_path,index=False,header=True
            )

            data_validation_artifact = DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.tested_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )
            return data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)

