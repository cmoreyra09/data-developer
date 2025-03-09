import  pandas as pd
from    sqlalchemy     import create_engine , text , inspect
from    sqlalchemy.orm import sessionmaker
from    sqlalchemy.exc import SQLAlchemyError
from    dotenv         import load_dotenv
import  os
import  re


class BulkInsertSQL():

    def __init__(self,server:str,database:str):
        
        self.server         = server
        self.database       = database
        self.engine         = None
        self.connection     = None


    def connectDB(self):

        try:
            connecString        = f'mssql+pyodbc://{self.server}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes'
            self.engine         = create_engine(connecString)
            self.connection     = self.engine.connect()
            print('Successful Connection.')
            
            return self.engine

        except SQLAlchemyError as e:
            print(f'Connetion error : {e} ')
            return None

   #    finally:
   #         self.connection.close()
   #         print('Connection closed successfully')

    def close_connect(self):
        
        try:
            if self.connection:
                self.connection.close()
                print('Disconnection successfully')
            else:
                print('No active connection to close') 
        
        except SQLAlchemyError as e:
            print(f'Disconnection Error {e}')

    def createDboTask(self,data,tableName,method): 

        columns = [f"[{re.sub(r'[ /()_-]+', '', i)[:24]}] NVARCHAR(MAX)" for i in data.columns] ## Replace Spaces Blank and Limited 24 Characteres for column 
        

        columns.extend([
                        "[FechaEjecucionInsert]     DATETIME        DEFAULT GETDATE()"   ,
                        "[UsuarioEjecucionInsert]   NVARCHAR(255)   DEFAULT SYSTEM_USER" ,
                        "[PcEjecucionInsert]        NVARCHAR(255)   DEFAULT HOST_NAME()"
                                ])


        inspector = inspect(self.engine)  # Fix: Corrected variable name
        table_exists = tableName in inspector.get_table_names()

        if method == 0:
            sqlAction = f'''
                CREATE TABLE [{tableName}] (
                    {"  ,  \n".join(columns)}
                );

            '''

        elif method == 1 and table_exists == True:
            sqlAction = f'TRUNCATE TABLE {tableName}'


        elif method == 2:
            sqlAction = f'''
            
            DROP TABLE {tableName};
                CREATE TABLE [{tableName}] (
                    {"  ,  \n".join(columns)}
                );
            '''
        
        else:
            raise ValueError('Metodo Invalido')
            
        with self.engine.connect() as connection:
            connection.execute(text(sqlAction))
            connection.commit()            
            print(f'The method was used {method}, execution : {sqlAction}')

        return sqlAction


if __name__ == "__main__":
    
    db = BulkInsertSQL('localhost','RawDataLake')
    engine = db.connectDB()


    df = pd.read_csv(r'../pipeline-performanceLoad/test_three.csv',delimiter=";",dtype=str,low_memory=False).head(5)
    createTable = db.createDboTask(df,'testing',0)
    print(createTable)

    db.close_connect()

