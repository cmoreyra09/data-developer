from    sqlalchemy         import create_engine, text
from    sqlalchemy.exc     import SQLAlchemyError
import  pyodbc
from    dotenv             import load_dotenv
import  os
import  csv
import  pandas as pd
import  datetime




load_dotenv()


class exportDataDB():

    def __init__(self):

        self.engine     = None
        self.connection = None

    def connectDB(self):

        try:
            connect_str     = os.getenv('DB_CONNECTION_STRING')
            self.engine     = create_engine(connect_str) ## Creamos el Engine
            self.connection = self.engine.connect() ## Nos Conectamos al database
            print('Conexion Exitosa')

            return self.engine

        except SQLAlchemyError as e :
            print(f'Error de conexion DB {e}')
            return None

    def close_connect(self):

        if self.connection:
            self.connection.close()
            print('Conexion Cerrada')


    def readFileSQL(self,filePath : str):

        try:
            with open(filePath,'r') as file:
                sqlQuery = file.read()
                # print(sqlQuery)
                print('Se leyo correctamente el query.')
                return sqlQuery

        except Exception as e:
            print(f'Error de Lectura del file SQL : {e}')

            return None

    def executeQuery(self,queryExport:str):
        

        try:
        
            with engine.connect() as connection:
                result      = connection.execute(text(queryExport))

                rows        = result.fetchall()
                headers     = result.keys()
                

                return rows , headers
            print('Consulta Ejecutado Correctamente.')
        
        except SQLAlchemyError as e:
            
            print(f'Error de ejecucion de la consulta. {e}')
            return None,None

    
    def exportDataToCsv(self,rows, headers , outputPath : str):


        try:
            if rows:

                with open(outputPath,'w',newline="",encoding='utf-8') as csvfile:
                    csv_writer = csv.writer(csvfile, delimiter="|")
                    csv_writer.writerow(headers)
                    csv_writer.writerows(rows)

                print(f'Se guardo correctamente en la ruta: {outputPath}')

        except Exception as e:
            print(f'Error de exportacion a csv {e}')
            return None


    def exportDataByPandas(self,rows,headers,outPath:str):


        try:
        
            if rows:
                df = pd.DataFrame(rows,columns=headers)
                
                df.to_csv(outPath, sep="|", index=False, encoding="utf-8")
                print(f"Datos guardados correctamente en: {outPath}")
            else:
                print('No hay datos para exportar')
        
        except Exception as e:

            print(f'Error en la exportacion con pandas.{e}')


if __name__ == "__main__":


    ## Configuration TimeFile

    try:
        currentDate     = datetime.datetime.today() - datetime.timedelta(days=1)
        dateExportData  = currentDate.strftime("%Y%m%d")
        
        db = exportDataDB()

        engine = db.connectDB()

        sqlQueryExport = db.readFileSQL('../pipeline-exportdata/query.sql')     

        ##print(sqlQueryExport)
        rows , headers = db.executeQuery(sqlQueryExport)


        df = pd.DataFrame(rows, columns=headers)
        print(df.head(5))  # 🔹 Mostrar DataFrame en consola


        db.exportDataToCsv(rows,headers,f'TestingOutputCSV_{dateExportData}.csv')
        db.exportDataByPandas(rows,headers,f'TestingOutputPandas_{dateExportData}.csv')
        db.close_connect()
    
    except Exception as e:
        print(f'Eror de ejecucion de proces. {e}')

    
    finally:
        db.close_connect()



