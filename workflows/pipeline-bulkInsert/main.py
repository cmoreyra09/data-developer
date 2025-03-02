import pandas as pd
import os
from   dotenv               import load_dotenv
from   sqlalchemy           import create_engine,text
from   sqlalchemy.exc       import SQLAlchemyError
import pyodbc


load_dotenv()


class  BulkInsertData():

    def __init__(self, tableName : str  = None):

        self.tableName      = tableName
        self.engine         = None
        self.connection     = None

    def connectDB(self):

        try:
            connect_str     = os.getenv('DB_CONNECTION_STRING')
            self.engine     = create_engine(connect_str)
            self.connection = self.engine.connect()

            print('Conexion Exitosa')

        except SQLAlchemyError as e:
            print(SQLAlchemyError)
    
    def close_connect(self):

        if self.connection:
            self.connection.close()
            print('Conexion Cerrada Exitosamente')


    def creatTableSQL(self,data):

        try:

            columns = []

            for i in data.columns:
                columns.append(f'[{i}] NVARCHAR(MAX)')

            # Columnas Auditorias

            columns.extend([
                "[FechaEjecucionInsert]     DATETIME        DEFAULT GETDATE()",
                "[UsuarioEjecucionInsert]   NVARCHAR(255)   DEFAULT SYSTEM_USER",
                "[PcEjecucionInsert]        NVARCHAR(255)   DEFAULT HOST_NAME()"
            ])

            columnsExtend = ", ".join(columns)

            queryTable = f"""
            IF NOT EXISTS (SELECT NAME FROM sys.tables WHERE name = '{self.tableName}')
            BEGIN
            
                CREATE TABLE [{self.tableName}] (
                
                            {",\n".join(columns)}
                    
                        );
            END
                    """

            print('Query Creado Exitosamente:')
            return  queryTable

        except Exception as e:

            print(f'Error en creacion de tabla. {e}')


    def executeQuery(self,queryExport:str):
        

        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(queryExport))
                connection.commit()
            print(f'Se ejecuto Correctamente el Query: {result}')

            return result

        except Exception as e:
            print(f'Se produjo un error : {e}')

            return None


    def ingestDataTask(self, data: pd.DataFrame, chunksize: int = 10000, method: str = 'replace'):

        rows = len(data)

        print(f"Iniciando ingesta de datos en '{self.tableName}'...")

        if method == 'replace':

            chunk = data.iloc[:chunksize]
            chunk.to_sql(name=self.tableName, con=self.connection, if_exists='replace', index=False)
            print(f"Ingestados {len(chunk)} registros en el primer bloque (modo replace)...")
            start_index = chunksize
        
        else:
        
            start_index = 0

        
        for i in range(start_index, rows, chunksize):
            chunk = data.iloc[i : min(i + chunksize, rows)]
            chunk_size = len(chunk)

            try:
                chunk.to_sql(name=self.tableName, con=self.connection, if_exists='append', index=False)
                print(f"Ingestados {chunk_size} registros en este bloque ({i+chunk_size}/{rows})...")
            except Exception as e:
                print(f"Error al insertar registros en el bloque {i}-{i+chunk_size}: {e}")
                break

        print(f"Proceso de Completado, se ingestaron {data.shape[0]} registros .")

    
    @staticmethod
    def readFileSQL(filePath : str):

        try:
            with open(filePath,'r') as file:
                sqlQuery = file.read()

                print('Se leyo correctamente el query')

            return sqlQuery
        
        except Exception as e:
            
            print(f'Error al leer el archivo SQL. {e}')
            
            return None

    @staticmethod
    def readPandasData(filepath):

        data = pd.read_csv(filepath,delimiter = ',').astype(str)

        return data



if __name__ == "__main__":

    db                  = BulkInsertData('Cliente')
    engine              = db.connectDB()
    getDataSet          = db.readPandasData(r'C:\Users\Carlos\Desktop\dataexport_ssis\Clientes.csv')
    ingesta             = db.ingestDataTask(getDataSet,chunksize=10000)
    db.close_connect()
















