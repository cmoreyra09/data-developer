import os
import shutil
import time


class SystemFileTask():

    def __init__(self, sourcePath:str = None, destinationPath:str = None):

        self.sourcePath         = sourcePath
        self.destinationPath    = destinationPath

    def setPaths(self,sourcePath:str,destinationPath:str):
        
        self.sourcePath         = sourcePath
        self.destinationPath    = destinationPath

    def createDir(self):

        try:
            if not self.sourcePath or not self.destinationPath:
                print('Error se debe asignar ambas rutas de creacion.')
                return None
        
            for path in [self.sourcePath, self.destinationPath]:
                if os.path.exists(path):
                    shutil.rmtree(path)
                    print(f'Carpeta Eliminadas: {path}')

                os.makedirs(path)
                print('Carpeta Creada Exitosamente.')

        except OSError as e:
            print(f'Error en : {e}')

    @staticmethod
    def copyFile(sourceInput,destinationOut):
        try:
            shutil.copy2(sourceInput,destinationOut)
            print('Archivo copiado correctamente ')
        
        except Exception as e:
            print(f'Error: {e}')

    @staticmethod
    def moveFile(sourceInput,destinationOut):
        shutil.move(sourceInput,destinationOut)
        print('Archivo movido correctamente')

    
    @staticmethod    
    def removeFile(sourceInput):
        os.remove(sourceInput)
        print('Archivo Eliminado correctamente')


if __name__ == "__main__":
    

    source          = r'../pipeline-systemTask/workby-python/Origen'
    destination     = r'../pipeline-systemTask/workby-python/Destino'
    inputFile       = r'../pipeline-systemTask/TestingOutputPandas_20250228.csv'
    
    origenFilePath  = r'../pipeline-systemTask/workby-python/Origen/TestingOutputPandas_20250228.csv'
    
    instance = SystemFileTask()
    instance.setPaths(source,destination)
    instance.createDir()
    
    ## (2)
    
    instance.copyFile(inputFile,source)
    ##instance.moveFile(origenFilePath,destination) Realizar un corte de archivo no dejandlo en el ruta principa
    instance.copyFile(origenFilePath,destination)

    
    time.sleep(10)
    
    ## (3)
    instance.removeFile(origenFilePath)


'''
🔹 shutil.copy(src, dst) → Copia el archivo sin metadatos.
🔹 shutil.copy2(src, dst) → Copia el archivo con metadatos.
🔹 shutil.move(src, dst) → Mueve el archivo en lugar de copiarlo.

'''
