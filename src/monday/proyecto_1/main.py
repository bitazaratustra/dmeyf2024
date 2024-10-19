from src.procesar_datos import transformar_datos

if __name__ == "__main__":
    dataset_path = 'data/raw/'  # Ruta donde está el archivo CSV original
    dataset_file = 'competencia_01_fe.csv'

    transformar_datos(dataset_path, dataset_file)
