import          pandas as pd
import          os
from            dotenv          import load_dotenv
from            sqlalchemy      import create_engine, text
from            sqlalchemy.exc  import SQLAlchemyError
import          pyodbc
import          time


class workloadToSQL():
    
    def __init__(self, server:str , database:str):
            
        self.server     = server
        self.database   = database
        self.engine     = None      ## Storage the conexion motor sql 
        self.connection = None  ## Sotrage the targetConexion


    def connectDB(self):

        try:
            connecString    = f'mssql+pyodbc://{self.server}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes'
            self.engine     = create_engine(connecString)
            self.connection = self.engine.connect()
            print('Conexion Exitosa')

        except SQLAlchemyError as e:
            print(f'Error de Conexion : {e}')

    def close_connect(self):

        try:

            if self.connection:
                self.connection.close()
                print('Conexion Cerrada Exitosamente')

        except SQLAlchemyError as e:
            print(f'Error al cerrar la conexion : {e}')


    def executeQuery(self, runQuery : str):

        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(runQuery))
                connection.commit()
            
            print('Se ejecuto Correctamente el Query.')

        except SQLAlchemyError as e:
            
            print(f'Error en la ejecucion del query : {e}')
            self.connection.close()
            print('Se cerro Correctamente la base de datos.')
            return None


    def loadIngestingTask(self, data : pd.DataFrame, dboName : str = None , chunksize : int = 10000 , method = 'replace'):


    
        rowsTotal = len(data)
        print(f'Iniciando Proceso de Ingesta para la tabla {dboName}')


        if method == 'replace':

            dataChunk = data.iloc[:chunksize]
            dataChunk.to_sql(name=dboName, con = self.connection, if_exists = 'replace',index=False)
            print(f'Ingestando datos en - modo Replace - cantidad de registros insertados {len(dataChunk)}')
            
            startIndex = chunksize


        else:
            startIndex = 0


        for i in range(startIndex,rowsTotal,chunksize):

            dataChunk = data.iloc[i: min(i+chunksize,rowsTotal)]

            ingestChunks = len(dataChunk)

            try:
                dataChunk.to_sql(name=dboName , con = self.connection , if_exists = 'append' , index=False)
                print(f'Datos Ingestados : {ingestChunks} / {i+ingestChunks} / de {rowsTotal}')

            except Exception as e:
                print(f'Error al insertar registro en el bloque {i} - {i+ingestChunks} : {e}')

        print(f'Proceso de Ingesta terminado, se ingreso {rowsTotal}')


    @staticmethod
    def readFilePandas(filepath:str):

        try:
            df = pd.read_csv(filepath, delimiter = ';', dtype=str, low_memory=False)
            df.replace({'nan': None, 'NaT':None}, inplace=True)
            df = df.map(lambda x: x[:-2] if isinstance(x, str) and x.endswith('.0') else x)
            print('Se leyo Correctamente el dataframe')

            return df 

        except Exception as e:
            print(f'Error en el proceso de lectura del input {e}')


    @staticmethod
    def readFileSQL(filepath:str):


        try:
            with open(filepath,'r') as file:
                sqlQuery = file.read()

                print(sqlQuery)

            return sqlQuery

        except Exception as e:
            print(f'Error en obtener queryFile')

            return None


if __name__ == "__main__":
    
    load_dotenv()

    TABLE_NANE =  'TestingPerformance'

    db = workloadToSQL(os.getenv('DB_SERVER'),os.getenv('DB_DATABASE'))
    dbEngine = db.connectDB()

    ## Extraction 
    data = db.readFilePandas(r'../pipeline-performanceLoad/test_three.csv').head(100)

    ## Load 
    db.loadIngestingTask(data,TABLE_NANE,1000)

    time.sleep(20)
    ## Transformation

    getQueryFile = db.readFileSQL(r'../pipeline-performanceLoad/transformation.sql')
    db.executeQuery(getQueryFile)

    db.close_connect()
