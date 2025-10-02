import pandas as pd
import mysql.connector
import boto3
import os
from datetime import datetime

def conectar_mysql():
    """Conecta a la base de datos MySQL"""
    try:
        conexion = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            user=os.getenv('MYSQL_USER', 'root'),
            password=os.getenv('MYSQL_PASSWORD', ''),
            database=os.getenv('MYSQL_DATABASE', 'test')
        )
        return conexion
    except Exception as e:
        print(f"Error conectando a MySQL: {e}")
        return None

def exportar_tabla_csv():
    """Lee todos los registros de una tabla y exporta a CSV"""
    conexion = conectar_mysql()
    if conexion is None:
        return None
    
    try:
        cursor = conexion.cursor()
        
        # Consulta todos los registros de una tabla
        # Cambia 'tu_tabla' por el nombre de tu tabla real
        cursor.execute("SELECT * FROM tu_tabla")
        resultados = cursor.fetchall()
        
        # Obtener nombres de columnas
        column_names = [i[0] for i in cursor.description]
        
        # Crear DataFrame y guardar como CSV
        df = pd.DataFrame(resultados, columns=column_names)
        nombre_archivo = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(nombre_archivo, index=False)
        
        print(f"Archivo {nombre_archivo} creado exitosamente")
        return nombre_archivo
        
    except Exception as e:
        print(f"Error exportando datos: {e}")
        return None
    finally:
        conexion.close()

def subir_s3(archivo):
    """Sube archivo CSV a bucket S3"""
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        
        bucket_name = os.getenv('S3_BUCKET', 'mi-bucket-ingesta')
        s3.upload_file(archivo, bucket_name, archivo)
        
        print(f"Archivo {archivo} subido a S3 bucket {bucket_name}")
        
    except Exception as e:
        print(f"Error subiendo a S3: {e}")

if __name__ == "__main__":
    # Exportar datos de MySQL a CSV
    archivo_csv = exportar_tabla_csv()
    
    if archivo_csv:
        # Subir a S3
        subir_s3(archivo_csv)
