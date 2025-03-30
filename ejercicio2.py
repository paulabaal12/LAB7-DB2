import pandas as pd
import numpy as np
import pymongo
import sqlalchemy
import json
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings('ignore')

def main():
    print("=== INICIANDO PROCESO ETL ===")
    
    try:
        # Crear y cargar tablas desde CSV
        print("\n1. PREPARANDO ENTORNO DE DATOS")
        create_and_load_tables_from_csv()
        
        # Cargar datos a MongoDB
        load_json_to_mongodb()

        print("\n2. EXTRACCIÓN, TRANSFORMACIÓN E INTEGRACIÓN DE DATOS")
        # 2.1 Ingestar datos de la base de datos relacional (SQL)
        print("\n2.1 Extracción de datos SQL")
        sql_df = extract_from_sql()
        if not sql_df.empty:
            sql_df = transform_sql_data(sql_df)
            print(f"Datos extraídos y transformados de SQL: {len(sql_df)} registros")
        else:
            print("No se pudieron extraer datos de SQL")
        
        # 2.2 Ingestar datos de la base de datos no relacional (MongoDB)
        print("\n2.2 Extracción de datos MongoDB")
        mongo_df = extract_from_mongodb()
        if not mongo_df.empty:
            mongo_df = transform_mongodb_data(mongo_df)
            print(f"Datos extraídos y transformados de MongoDB: {len(mongo_df)} registros")
        else:
            print("No se pudieron extraer datos de MongoDB")
        
        # 2.3 Integrar ambos conjuntos de datos
        print("\n2.3 Integración de datos")
        if not sql_df.empty and not mongo_df.empty:
            integrated_df = integrate_data(sql_df, mongo_df)
            print(f"Datos integrados: {len(integrated_df)} registros")
        else:
            integrated_df = pd.DataFrame()
            print("No se pudo realizar la integración debido a falta de datos en alguna fuente")
        
        # 2.4 Cargar los datos integrados en el data warehouse
        print("\n2.4 Carga de datos en el data warehouse")
        if not integrated_df.empty:
            load_to_data_warehouse(integrated_df)
            
            # Generar insights
            print("\n3. ANÁLISIS Y GENERACIÓN DE INSIGHTS")
            insights = generate_insights(integrated_df)
            for i, insight in enumerate(insights, 1):
                print(f"\n{insight}")
        else:
            print("No se cargarán datos en el data warehouse debido a errores previos")
    
    except Exception as e:
        print(f"Error en el proceso ETL: {str(e)}")

    print("\n=== PROCESO ETL FINALIZADO ===")

# 2.1 Extraer y transformar datos de SQL
def extract_from_sql():
    try:
        # Configurar conexión a la base de datos PostgreSQL
        sql_connection_string = "postgresql+psycopg2://postgres:resusan120104@localhost:5432/lab07?client_encoding=utf8"
        sql_engine = create_engine(sql_connection_string)
        
        # Primero, inspeccionemos los nombres de las columnas en ambas tablas
        with sql_engine.connect() as connection:
            # Verificar columnas en la tabla paises
            cols_paises = pd.read_sql_query(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'paises'"), connection)
            print("Columnas en tabla paises:", cols_paises['column_name'].tolist())
            
            # Verificar columnas en la tabla envejecimiento
            cols_env = pd.read_sql_query(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'envejecimiento'"), connection)
            print("Columnas en tabla envejecimiento:", cols_env['column_name'].tolist())
            
            # Consulta modificada para obtener todos los datos de ambas tablas
            query = """
            SELECT e.nombre_pais, e.continente, e.poblacion, e.tasa_de_envejecimiento
            FROM envejecimiento e
            """
            
            # Extraer datos
            sql_df = pd.read_sql_query(text(query), connection)
        
        print(f"Conexión exitosa a la base de datos PostgreSQL. Datos extraídos: {len(sql_df)} registros")
        # Verificar que tasa_de_envejecimiento tiene datos
        non_null_count = sql_df['tasa_de_envejecimiento'].notna().sum()
        print(f"Registros con valores en tasa_de_envejecimiento: {non_null_count}")
        
        return sql_df
        
    except Exception as e:
        print(f"Error al extraer datos de PostgreSQL: {str(e)}")
        # Retornar DataFrame vacío en caso de error
        return pd.DataFrame()
    
def transform_sql_data(sql_df):
    if sql_df.empty:
        return sql_df
    
    try:
        # Eliminar duplicados
        sql_df = sql_df.drop_duplicates()
        
        # Convertir nombres de países a formato estándar
        sql_df['pais'] = sql_df['pais'].str.strip().str.title()
        
        # Convertir continentes a formato estándar
        sql_df['continente'] = sql_df['continente'].str.strip().str.title()
        
        # Manejar valores nulos o incorrectos
        sql_df['poblacion'] = pd.to_numeric(sql_df['poblacion'], errors='coerce')
        sql_df['tasa_de_envejecimiento'] = pd.to_numeric(sql_df['tasa_de_envejecimiento'], errors='coerce')
        
        # Filtrar valores no válidos
        sql_df = sql_df.dropna(subset=['pais', 'continente'])
        
        print(f"Transformación de datos SQL completada: {len(sql_df)} registros limpios")
        return sql_df
        
    except Exception as e:
        print(f"Error al transformar datos de SQL: {str(e)}")
        return sql_df

def load_json_to_mongodb():
    try:
        # Conexión a MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        mongo_db = mongo_client["lab7"]
        turismo_collection = mongo_db["turismo"]
        precios_collection = mongo_db["precios_big_mac"]
        
        turismo_collection.delete_many({})
        precios_collection.delete_many({})
        
        # DJSON files 
        json_files = [
            "./Datos_para_MongoDB/costos_turisticos_africa.json",
            "./Datos_para_MongoDB/costos_turisticos_america.json",
            "./Datos_para_MongoDB/costos_turisticos_asia.json",
            "./Datos_para_MongoDB/costos_turisticos_europa.json"
        ]
        
        # Cargar datos de turismo a MongoDB
        total_docs = 0
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    # Verificar explícitamente el tipo de datos cargados
                    print(f"Tipo de datos cargados de {json_file}: {type(data)}")
                    print(f"Contenido de muestra: {data[:1] if isinstance(data, list) else data}")
                    
                    if isinstance(data, list):
                        # Insertar los documentos uno por uno para mejor control
                        for doc in data:
                            turismo_collection.insert_one(doc)
                        total_docs += len(data)
                        print(f"Insertados {len(data)} documentos de {json_file}")
                    else:
                        turismo_collection.insert_one(data)
                        total_docs += 1
                        print(f"Insertado 1 documento de {json_file}")
            except Exception as e:
                print(f"Error detallado al cargar {json_file}: {str(e)}")
                import traceback
                traceback.print_exc()  # Muestra el traceback completo para depuración
        
        # Cargar datos de precios Big Mac a MongoDB
        try:
            with open("./Datos_para_MongoDB/paises_mundo_big_mac.json", 'r', encoding='utf-8') as file:
                big_mac_data = json.load(file)
                print(f"Tipo de datos Big Mac: {type(big_mac_data)}")
                precios_collection.insert_many(big_mac_data)
                print(f"Loaded {len(big_mac_data)} Big Mac price records to MongoDB")
        except Exception as e:
            print(f"Error loading Big Mac prices to MongoDB: {str(e)}")
        
        print(f"Successfully loaded {total_docs} tourism documents and Big Mac data to MongoDB")
        
        # Verificar lo que está en la colección después de la carga
        turismo_count = turismo_collection.count_documents({})
        precios_count = precios_collection.count_documents({})
        print(f"Documentos en colección turismo: {turismo_count}")
        print(f"Documentos en colección precios_big_mac: {precios_count}")
        
    except Exception as e:
        print(f"Error loading data to MongoDB: {str(e)}")

# 2.2 Extraer y transformar datos de MongoDB
def extract_from_mongodb():
    try:
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        mongo_db = mongo_client["lab7"]

        # Extraer datos de turismo
        turismo_collection = mongo_db["turismo"]
        turismo_data = list(turismo_collection.find({}, {'_id': 0}))
        print(f"Documentos extraídos de turismo: {len(turismo_data)}")

        # Extraer datos de precios Big Mac
        precios_collection = mongo_db["precios_big_mac"]
        precios_data = list(precios_collection.find({}, {'_id': 0}))
        print(f"Documentos extraídos de precios_big_mac: {len(precios_data)}")

        if not turismo_data and not precios_data:
            print("No se encontraron datos en las colecciones de MongoDB")
            return pd.DataFrame()

        # Convertir a DataFrames
        turismo_df = pd.DataFrame(turismo_data) if turismo_data else pd.DataFrame()
        precios_df = pd.DataFrame(precios_data) if precios_data else pd.DataFrame()

        # Asegurar que 'país' se llame 'pais'
        if 'país' in turismo_df.columns:
            turismo_df.rename(columns={'país': 'pais'}, inplace=True)

        if 'país' in precios_df.columns:
            precios_df.rename(columns={'país': 'pais'}, inplace=True)

        # Imprimir estructuras para depuración
        print("Estructura de turismo_df \n")
        print(turismo_df.head())
        print("Estructura de precios_df\n")
        print(precios_df.head())

        # Fusionar
        if not turismo_df.empty and not precios_df.empty:
            mongo_df = pd.merge(
                turismo_df,
                precios_df[['pais', 'precio_big_mac_usd']],
                on='pais',
                how='outer'
            )
        elif not turismo_df.empty:
            mongo_df = turismo_df
        elif not precios_df.empty:
            mongo_df = precios_df
        else:
            mongo_df = pd.DataFrame()

        print(f"Registros combinados después del merge: {len(mongo_df)}")
        return mongo_df

    except Exception as e:
        print(f"Error extrayendo datos de MongoDB: {str(e)}")
        return pd.DataFrame()

def transform_mongodb_data(mongo_df):
    if mongo_df.empty:
        return mongo_df
    
    try:
        # Eliminar duplicados
        if 'costos_diarios_estimados_en_dolares' in mongo_df.columns:
            print("Procesando columna de costos...")
            
            categories = ['hospedaje', 'comida', 'transporte', 'entretenimiento']
            price_levels = ['precio_bajo_usd', 'precio_promedio_usd', 'precio_alto_usd']
            
            for category in categories:
                for level in price_levels:
                    col_name = f'{category}.{level}'
                    
                    def extract_price(x):
                        try:
                            if isinstance(x, dict) and category in x:
                                if level in x[category]:
                                    return x[category][level]
                            return None
                        except Exception:
                            return None
                    
                    mongo_df[col_name] = mongo_df['costos_diarios_estimados_en_dolares'].apply(extract_price)
            
            # Calcula r el costo promedio total
            mongo_df['costo_promedio_total'] = mongo_df[[f'{cat}.precio_promedio_usd' 
                                                        for cat in categories]].sum(axis=1, skipna=True)
            
            # Eliminar la columna original de costos
            mongo_df.drop(columns=['costos_diarios_estimados_en_dolares'], inplace=True)
        
        mongo_df = mongo_df.drop_duplicates()

        mongo_df.columns = [col.lower().replace(' ', '_') for col in mongo_df.columns]

        if 'pais' in mongo_df.columns:
            mongo_df['pais'] = mongo_df['pais'].str.strip().str.title()

        if 'precio_big_mac_usd_usd' in mongo_df.columns:
            mongo_df.rename(columns={'precio_big_mac_usd_usd': 'precio_big_mac_usd'}, inplace=True)

        numeric_columns = [col for col in mongo_df.columns if any(term in col for term in ['costo', 'precio', 'usd'])]
        for col in numeric_columns:
            mongo_df[col] = pd.to_numeric(mongo_df[col], errors='coerce')

        print(f"Transformación de datos MongoDB completada. {len(mongo_df)} registros procesados.")
        print(f"Columnas finales: {mongo_df.columns.tolist()}")
        return mongo_df

    except Exception as e:
        import traceback
        print(f"Error al transformar los datos de MongoDB: {str(e)}")
        traceback.print_exc()
        return mongo_df

    
# 2.3 Integrar los datos de ambas fuentes
def integrate_data(sql_df, mongo_df):
    try:
        if sql_df.empty or mongo_df.empty:
            raise ValueError("Al menos uno de los DataFrames está vacío, no se puede realizar la integración")
        
        print("Columnas en SQL DataFrame:", sql_df.columns.tolist())
        print("Columnas en MongoDB DataFrame:", mongo_df.columns.tolist())
        
        # Crear una copia de los dataframes originales para no modificarlos
        sql_df_clean = sql_df.copy()
        mongo_df_clean = mongo_df.copy()
        
        # Función para normalizar nombres de países
        def normalize_country_name(country):
            if not isinstance(country, str):
                return country
            
            # Eliminar acentos y espacios extras
            import unicodedata
            country = unicodedata.normalize('NFKD', country).encode('ASCII', 'ignore').decode('utf-8').strip()
            
            # Convertir todo a minúscula para normalizaciones adicionales
            country_lower = country.lower()
            
            # Diccionario más completo de mapeo de países
            name_map_case_insensitive = {
                # America
                'united states': 'USA', 
                'united states of america': 'USA',
                'estados unidos': 'USA',
                'us': 'USA',
                'usa': 'USA',
                'u.s.a.': 'USA',
                'u.s.': 'USA',
                
                # Asia
                'south korea': 'Korea',
                'corea del sur': 'Korea',
                'republic of korea': 'Korea',
                'korea, south': 'Korea',
                'korea, republic of': 'Korea',
                'korea': 'Korea',
                'north korea': 'North Korea',
                'corea del norte': 'North Korea',
                
                # Europa
                'russian federation': 'Russia',
                'federacion rusa': 'Russia',
                'russia': 'Russia',
                
                'united kingdom': 'UK',
                'reino unido': 'UK',
                'great britain': 'UK',
                'england': 'UK',
                'uk': 'UK',
                'u.k.': 'UK',
                
                #  Bosnia
                'bosnia and herzegovina': 'Bosnia and Herzegovina',
                'bosnia & herzegovina': 'Bosnia and Herzegovina',
                'bosnia': 'Bosnia and Herzegovina',
                'herzegovina': 'Bosnia and Herzegovina',
                
                
                'czechia': 'Czech Republic',
                'czech republic': 'Czech Republic',
            }
            
            # Buscar coincidencia en el diccionario (insensible a mayúsculas/minúsculas)
            normalized_country = name_map_case_insensitive.get(country_lower)
            
            if normalized_country:
                return normalized_country
            else:
                # Si no hay coincidencia en el mapeo, aplicar formato de título estándar
                # Pero manejar palabras como "and", "of", etc. correctamente
                words = country.split()
                if len(words) > 1:
                    minor_words = ['and', 'of', 'the', 'du', 'de', 'del', 'la', 'el']
                    titled_words = []
                    for i, word in enumerate(words):
                        if i > 0 and word.lower() in minor_words:
                            titled_words.append(word.lower())
                        else:
                            titled_words.append(word.capitalize())
                    return ' '.join(titled_words)
                else:
                    # Para nombres de una sola palabra
                    return country.capitalize()
        
        # Normalizar nombres de países en ambos DataFrames
        if 'nombre_pais' in sql_df_clean.columns:
            sql_df_clean['nombre_pais'] = sql_df_clean['nombre_pais'].apply(normalize_country_name)
            sql_df_clean['pais'] = sql_df_clean['nombre_pais']
        elif 'pais' in sql_df_clean.columns:
            sql_df_clean['pais'] = sql_df_clean['pais'].apply(normalize_country_name)
            
        if 'pais' in mongo_df_clean.columns:
            mongo_df_clean['pais'] = mongo_df_clean['pais'].apply(normalize_country_name)
        
        # Normalizar nombres de columnas
        def normalize_column_name(col_name):
            import unicodedata
            normalized = unicodedata.normalize('NFKD', col_name).encode('ASCII', 'ignore').decode('utf-8')
            normalized = normalized.lower().replace(' ', '_')
            return normalized
        
        # Normalizar nombres de columnas
        sql_df_clean.columns = [normalize_column_name(col) for col in sql_df_clean.columns]
        mongo_df_clean.columns = [normalize_column_name(col) for col in mongo_df_clean.columns]
        
        # Mostrar países antes del merge para verificar la normalización
        print("Muestra de países normalizados en SQL DataFrame:", sql_df_clean['pais'].sample(min(5, len(sql_df_clean))).tolist())
        print("Muestra de países normalizados en MongoDB DataFrame:", mongo_df_clean['pais'].sample(min(5, len(mongo_df_clean))).tolist())
        
        # Ver si tenemos "Bosnia" o "Korea" en alguno de los DataFrames
        if 'Bosnia' in ' '.join(sql_df_clean['pais'].astype(str)):
            print("Países que contienen 'Bosnia' en SQL:", sql_df_clean[sql_df_clean['pais'].astype(str).str.contains('Bosnia', case=False)]['pais'].tolist())
        if 'Bosnia' in ' '.join(mongo_df_clean['pais'].astype(str)):
            print("Países que contienen 'Bosnia' en MongoDB:", mongo_df_clean[mongo_df_clean['pais'].astype(str).str.contains('Bosnia', case=False)]['pais'].tolist())
        
        if 'Korea' in ' '.join(sql_df_clean['pais'].astype(str)):
            print("Países que contienen 'Korea' en SQL:", sql_df_clean[sql_df_clean['pais'].astype(str).str.contains('Korea', case=False)]['pais'].tolist())
        if 'Korea' in ' '.join(mongo_df_clean['pais'].astype(str)):
            print("Países que contienen 'Korea' en MongoDB:", mongo_df_clean[mongo_df_clean['pais'].astype(str).str.contains('Korea', case=False)]['pais'].tolist())
        
        # Verificar que la columna tasa_de_envejecimiento está presente y tiene datos
        if 'tasa_de_envejecimiento' in sql_df_clean.columns:
            non_null_count = sql_df_clean['tasa_de_envejecimiento'].notna().sum()
            print(f"Antes de merge, registros con tasa_de_envejecimiento no nula: {non_null_count}")
        
        # Eliminar duplicados basados en país antes del merge
        sql_df_clean = sql_df_clean.drop_duplicates(subset=['pais'])
        mongo_df_clean = mongo_df_clean.drop_duplicates(subset=['pais'])
        
        # Eliminar 'nombre_pais' si existe y ya tenemos 'pais'
        if 'nombre_pais' in sql_df_clean.columns and 'pais' in sql_df_clean.columns:
            sql_df_clean = sql_df_clean.drop('nombre_pais', axis=1)
            
        # Realizar unión (merge) de los DataFrames
        integrated_df = pd.merge(
            sql_df_clean,
            mongo_df_clean,
            on='pais',
            how='outer'
        )
        
        # Manejar columnas duplicadas que pueden surgir del merge
        for col in integrated_df.columns:
            if col.endswith('_x') or col.endswith('_y'):
                base_col = col[:-2]
                # Si ya existe la columna base, saltamos
                if base_col in integrated_df.columns:
                    continue
                    
                # Si existen ambas versiones _x y _y
                x_col = f"{base_col}_x"
                y_col = f"{base_col}_y"
                if x_col in integrated_df.columns and y_col in integrated_df.columns:
                    # Combinar preferenciando valores no nulos
                    integrated_df[base_col] = integrated_df[x_col].combine_first(integrated_df[y_col])
                    integrated_df = integrated_df.drop([x_col, y_col], axis=1)
                # Si solo existe una versión
                elif x_col in integrated_df.columns:
                    integrated_df = integrated_df.rename(columns={x_col: base_col})
                elif y_col in integrated_df.columns:
                    integrated_df = integrated_df.rename(columns={y_col: base_col})
        
        # Verificar que la columna tasa_de_envejecimiento todavía tiene datos
        if 'tasa_de_envejecimiento' in integrated_df.columns:
            non_null_count = integrated_df['tasa_de_envejecimiento'].notna().sum()
            print(f"Después de merge, registros con tasa_de_envejecimiento no nula: {non_null_count}")
            
            # Imprimir algunos ejemplos de países con valores no nulos
            sample_countries = integrated_df[integrated_df['tasa_de_envejecimiento'].notna()]['pais'].tolist()[:5]
            print(f"Ejemplos de países con tasa_de_envejecimiento: {sample_countries}")
        
        # Eliminar posibles duplicados de filas
        integrated_df = integrated_df.drop_duplicates(subset=['pais'])
        
        # Eliminar columnas completamente vacías
        integrated_df = integrated_df.dropna(axis=1, how='all')
        
        print(f"Integración de datos completada: {len(integrated_df)} registros combinados")
        print(f"Columnas finales: {integrated_df.columns.tolist()}")
        
        return integrated_df
        
    except Exception as e:
        print(f"Error al integrar datos: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    
# 2.4 Cargar los datos integrados en el data warehouse
def load_to_data_warehouse(integrated_df):
    try:
        if integrated_df.empty:
            raise ValueError("No hay datos para cargar en el data warehouse")
        
        # Keep all columns - no filtering
        clean_df = integrated_df.copy()
        
        # Print column dtypes for debugging
        print("Tipos de datos antes de la conversión:")
        for col in clean_df.columns:
            print(f"{col}: {clean_df[col].dtype}, Ejemplos: {clean_df[col].head(3).tolist()}")
            
        # Special handling for tasa_de_envejecimiento
        if 'tasa_de_envejecimiento' in clean_df.columns:
            # First ensure it's numeric
            clean_df['tasa_de_envejecimiento'] = pd.to_numeric(clean_df['tasa_de_envejecimiento'], errors='coerce')
            print(f"Estadísticas de tasa_de_envejecimiento después de conversión: \n{clean_df['tasa_de_envejecimiento'].describe()}")
            print(f"Valores nulos: {clean_df['tasa_de_envejecimiento'].isna().sum()} de {len(clean_df)}")
            
            # Optional: Show sample of countries with null values
            null_countries = clean_df[clean_df['tasa_de_envejecimiento'].isna()]['pais'].tolist()[:5]
            print(f"Ejemplos de países con tasa_de_envejecimiento nulos: {null_countries}")
        
        # Convert to appropriate data types
        for col in clean_df.columns:
            # If likely numeric column
            if any(term in col.lower() for term in ['precio', 'costo', 'tasa', 'poblacion', 'usd']):
                clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
            # For text columns, ensure they are strings
            else:
                # Skip conversion for any complex objects
                if clean_df[col].dtype != 'object' or clean_df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    continue
                clean_df[col] = clean_df[col].astype(str)
        
        # Drop rows where all values are null
        clean_df = clean_df.dropna(how='all')
        
        # orden de columnas 
        ordered_columns = []
    
        basic_info = ['pais', 'capital', 'continente', 'region', 'poblacion', 'tasa_de_envejecimiento']
        for col in basic_info:
            if col in clean_df.columns:
                ordered_columns.append(col)
        
        # Big Mac precio
        if 'precio_big_mac_usd' in clean_df.columns:
            ordered_columns.append('precio_big_mac_usd')
            
        # turismo
        tourism_categories = ['hospedaje', 'comida', 'transporte', 'entretenimiento']
        for category in tourism_categories:
            for level in ['precio_bajo_usd', 'precio_promedio_usd', 'precio_alto_usd']:
                col = f'{category}.{level}'
                if col in clean_df.columns:
                    ordered_columns.append(col)
                    
        # costo promedio total 
        if 'costo_promedio_total' in clean_df.columns:
            ordered_columns.append('costo_promedio_total')
        
        ## Asegurarse de que todas las columnas estén en el orden correcto
        remaining_columns = [col for col in clean_df.columns if col not in ordered_columns]
        ordered_columns.extend(remaining_columns)
            
        # Reorder columns
        clean_df = clean_df[ordered_columns]
        
        clean_df.columns = [col.replace('.', '_') for col in clean_df.columns]
        
        # Exportación de CSV 
        csv_path = "./paises_datos_integrados.csv"
        clean_df.to_csv(csv_path, index=False)
        print(f"Datos exportados a CSV: {csv_path}")
        
        # data warehouse
        warehouse_connection = "postgresql+psycopg2://postgres:resusan120104@localhost:5432/lab07?client_encoding=utf8"
        warehouse_engine = create_engine(warehouse_connection)
        target_table = "paises_datos_integrados"
        
        # CCrea tabla si no hay 
        with warehouse_engine.connect() as connection:
        
            column_defs = []
            for col in clean_df.columns:
                col_safe = col  
                
                if col == 'pais':
                    column_defs.append(f"{col_safe} VARCHAR(255)")
                elif col in ['continente', 'region', 'capital']:
                    column_defs.append(f"{col_safe} VARCHAR(255)")
                elif col == 'poblacion':
                    column_defs.append(f"{col_safe} BIGINT")
                elif col == 'tasa_de_envejecimiento':
                    column_defs.append(f"{col_safe} FLOAT")  
                else:
                    column_defs.append(f"{col_safe} FLOAT")
            
            columns_sql = ", ".join(column_defs)
            
            connection.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
            
            create_table_query = f"""
            CREATE TABLE {target_table} (
                id SERIAL PRIMARY KEY,
                {columns_sql}
            );
            """
            connection.execute(text(create_table_query))
            print(f"Tabla '{target_table}' creada en el data warehouse")
        
        # Load data into table
        clean_df.to_sql(target_table, warehouse_engine, if_exists='append', index=False)
        print(f"Datos cargados exitosamente: {len(clean_df)} registros en la tabla '{target_table}' del data warehouse")
        
    except Exception as e:
        import traceback
        print(f"Error al cargar datos en el data warehouse: {str(e)}")
        traceback.print_exc()

# Generar insights de los datos integrados
def generate_insights(integrated_df):
    insights = []
    
    try:
        if integrated_df.empty:
            return ["No hay datos suficientes para generar insights"]
        
        # INSIGHT 1: Relación entre envejecimiento y costos turísticos
        if 'tasa_de_envejecimiento' in integrated_df.columns and any('costo' in col for col in integrated_df.columns):
            # Identificar columna de costo apropiada
            costo_col = next((col for col in integrated_df.columns if 'costo_promedio' in col), None)
            
            if costo_col:
                # Filtrar datos válidos
                valid_data = integrated_df.dropna(subset=['tasa_de_envejecimiento', costo_col])
                
                if len(valid_data) > 5:  # Asegurar suficientes datos para el análisis
                    correlation = valid_data['tasa_de_envejecimiento'].corr(valid_data[costo_col])
                    
                    insight_text = f"INSIGHT 1: La correlación entre el índice de envejecimiento y el {costo_col.replace('_', ' ')} es {correlation:.2f}."
                    if correlation > 0.5:
                        insight_text += " Existe una correlación positiva fuerte, lo que sugiere que los países con poblaciones más envejecidas tienden a tener costos turísticos más altos."
                    elif correlation > 0.2:
                        insight_text += " Existe una correlación positiva moderada, lo que sugiere una tendencia donde los países con poblaciones más envejecidas pueden tener costos turísticos ligeramente más altos."
                    elif correlation > -0.2:
                        insight_text += " No existe una correlación significativa, lo que sugiere que la edad de la población no es un factor determinante en los costos turísticos."
                    elif correlation > -0.5:
                        insight_text += " Existe una correlación negativa moderada, lo que sugiere que los países con poblaciones más envejecidas tienden a tener costos turísticos ligeramente más bajos."
                    else:
                        insight_text += " Existe una correlación negativa fuerte, lo que sugiere que los países con poblaciones más envejecidas tienden a tener costos turísticos significativamente más bajos."
                    
                    insights.append(insight_text)
        
        # INSIGHT 2: Relación entre precio del Big Mac y tamaño de población
        if 'precio_big_mac_usd' in integrated_df.columns and 'poblacion' in integrated_df.columns:
            # Filtrar datos válidos
            valid_data = integrated_df.dropna(subset=['precio_big_mac_usd', 'poblacion'])
            
            if len(valid_data) > 5:  # Asegurar suficientes datos para el análisis
                # Categorizar países por tamaño de población
                valid_data['categoria_poblacion'] = pd.cut(
                    valid_data['poblacion'], 
                    bins=[0, 5e6, 20e6, 100e6, float('inf')],
                    labels=['Pequeño (<5M)', 'Mediano (5-20M)', 'Grande (20-100M)', 'Muy Grande (>100M)']
                )
                
                # Calcular precio promedio del Big Mac por categoría
                big_mac_by_pop = valid_data.groupby('categoria_poblacion')['precio_big_mac_usd'].mean().sort_values()
                
                insight_text = "INSIGHT 2: Precio promedio del Big Mac según el tamaño de la población del país:\n"
                for category, price in big_mac_by_pop.items():
                    insight_text += f"- {category}: ${price:.2f} USD\n"
                
                # Añadir interpretación
                min_category = big_mac_by_pop.idxmin()
                max_category = big_mac_by_pop.idxmax()
                diff_percent = ((big_mac_by_pop.max() - big_mac_by_pop.min()) / big_mac_by_pop.min()) * 100
                
                insight_text += f"\nLos países {max_category} tienen un precio de Big Mac {diff_percent:.1f}% más alto que los países {min_category}."
                insight_text += " Esto podría indicar diferencias en el poder adquisitivo, costos de importación, o estrategias de precios de McDonald's según el tamaño del mercado."
                
                insights.append(insight_text)
        
        # INSIGHT 3: Análisis por continente o región de costos turísticos y precios de Big Mac
        continent_col = 'continente' if 'continente' in integrated_df.columns else 'region' if 'region' in integrated_df.columns else None
        
        if continent_col and 'precio_big_mac_usd' in integrated_df.columns and any('costo' in col for col in integrated_df.columns):
            # Identificar columna de costo apropiada
            costo_col = next((col for col in integrated_df.columns if 'costo_promedio' in col), 
                           next((col for col in integrated_df.columns if 'costo_' in col), None))
            
            if costo_col:
                # Filtrar datos válidos
                valid_data = integrated_df.dropna(subset=[continent_col, 'precio_big_mac_usd'])
                
                if len(valid_data) > 5:  # Asegurar suficientes datos para el análisis
                    # Calcular precios promedios por continente/región
                    region_prices = valid_data.groupby(continent_col)['precio_big_mac_usd'].mean().sort_values(ascending=False)
                    
                    insight_text = f"INSIGHT 3: {continent_col.title()} ordenados por precio promedio del Big Mac:\n"
                    for region, price in region_prices.items():
                        insight_text += f"- {region}: ${price:.2f} USD\n"
                    
                    # Añadir interpretación para Big Mac
                    most_expensive = region_prices.index[0]
                    least_expensive = region_prices.index[-1]
                    diff_percent = ((region_prices.max() - region_prices.min()) / region_prices.min()) * 100
                    
                    insight_text += f"\n{most_expensive} es {diff_percent:.1f}% más caro que {least_expensive} en términos de precios de Big Mac."
                    insight_text += " Esta disparidad de precios puede reflejar diferencias en el coste de vida, poder adquisitivo y estrategias de fijación de precios regionales."
                    
                    # Calcular relación entre costo turístico y precio de Big Mac si hay datos disponibles
                    if costo_col and costo_col in valid_data.columns:
                        valid_cost_data = valid_data.dropna(subset=[costo_col])
                        if len(valid_cost_data) > 5:
                            correlation = valid_cost_data['precio_big_mac_usd'].corr(valid_cost_data[costo_col])
                            insight_text += f"\n\nLa correlación entre el precio del Big Mac y los costos turísticos es {correlation:.2f}, "
                            if correlation > 0.5:
                                insight_text += "indicando una fuerte relación entre ambos indicadores económicos. El índice Big Mac parece ser un buen predictor del costo turístico en estos países."
                            elif correlation > 0.2:
                                insight_text += "mostrando una relación moderada. El precio del Big Mac puede ofrecer alguna orientación sobre los costos turísticos, aunque con excepciones."
                            elif correlation > -0.2:
                                insight_text += "lo que sugiere que no hay una relación clara entre ambos. El precio del Big Mac no parece ser un buen indicador de los costos turísticos."
                            else:
                                insight_text += "mostrando una relación inversa. Los países con Big Macs más costosos tienden a tener menores costos turísticos, posiblemente debido a factores como subsidios al turismo o impuestos específicos al sector alimentario."
                    
                    insights.append(insight_text)
        
        # Si no se generaron insights, agregar un mensaje predeterminado
        if not insights:
            insights.append("No fue posible generar insights suficientes debido a la estructura o calidad de los datos. Se recomienda revisar la integración y limpieza de los datos.")
        
    except Exception as e:
        print(f"Error al generar insights: {str(e)}")
        insights.append("Error al generar insights: se produjo una excepción durante el análisis.")
    
    return insights

def create_and_load_tables_from_csv():
    try:
        # Configurar conexión a PostgreSQL
        warehouse_connection = "postgresql+psycopg2://postgres:resusan120104@localhost:5432/lab07?client_encoding=utf8"
        warehouse_engine = create_engine(warehouse_connection)

        # Archivos CSV y nombres de tablas
        csv_files = {
            "pais_poblacion.csv": "paises",
            "pais_envejecimiento.csv": "envejecimiento"
        }

        for csv_file, table_name in csv_files.items():
            # Leer el archivo CSV
            file_path = f"./Datos_para_SQL/{csv_file}"  # Ajusta la ruta si es necesario
            df = pd.read_csv(file_path)

            # Crear tabla y cargar datos
            df.to_sql(table_name, warehouse_engine, if_exists="replace", index=False)
            print(f"Tabla '{table_name}' creada y datos cargados desde '{csv_file}'")

    except Exception as e:
        print(f"Error al crear y cargar tablas desde CSV: {str(e)}")

def check_source_data_for_nulls():
    try:
    
        sql_connection_string = "postgresql+psycopg2://postgres:resusan120104@localhost:5432/lab07?client_encoding=utf8"
        sql_engine = create_engine(sql_connection_string)
        
        with sql_engine.connect() as connection:
            query = "SELECT nombre_pais, tasa_de_envejecimiento FROM envejecimiento WHERE tasa_de_envejecimiento IS NOT NULL LIMIT 10"
            sample_data = pd.read_sql(query, connection)
            print("Muestra de datos de tasa_de_envejecimiento en la tabla fuente:")
            print(sample_data)
            
            count_query = "SELECT COUNT(*) as total, COUNT(tasa_de_envejecimiento) as with_age_rate FROM envejecimiento"
            counts = pd.read_sql(count_query, connection)
            print("\nEstadísticas de valores en la tabla fuente:")
            print(counts)
    except Exception as e:
        print(f"Error al verificar datos fuente: {str(e)}")

main()