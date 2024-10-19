
import duckdb
import pandas as pd

def transformar_datos(dataset_path, dataset_file):
    con = duckdb.connect(database=':memory:')

    con.execute(f"""
    CREATE OR REPLACE TABLE competencia_01_crudo AS
    SELECT *
    FROM read_csv_auto('{dataset_path + dataset_file}');
    """)

    con.execute("""
    CREATE OR REPLACE TABLE clientes_por_mes AS
    SELECT DISTINCT numero_de_cliente, foto_mes
    FROM competencia_01_crudo;
    """)

    con.execute("""
    CREATE OR REPLACE TABLE clientes_estado AS
    WITH clientes_foto_mes AS (
        SELECT
            c1.numero_de_cliente,
            c1.foto_mes AS foto_mes_actual,
            c2.foto_mes AS foto_mes_siguiente,
            c3.foto_mes AS foto_mes_dos_meses
        FROM clientes_por_mes c1
        LEFT JOIN clientes_por_mes c2
            ON c1.numero_de_cliente = c2.numero_de_cliente
            AND c2.foto_mes = c1.foto_mes + 1
        LEFT JOIN clientes_por_mes c3
            ON c1.numero_de_cliente = c3.numero_de_cliente
            AND c3.foto_mes = c1.foto_mes + 2
    )
    SELECT
        numero_de_cliente,
        foto_mes_actual,
        CASE
            WHEN foto_mes_siguiente IS NOT NULL AND foto_mes_dos_meses IS NOT NULL THEN 'CONTINUA'
            WHEN foto_mes_siguiente IS NULL AND foto_mes_dos_meses IS NOT NULL THEN 'BAJA+1'
            WHEN foto_mes_siguiente IS NULL AND foto_mes_dos_meses IS NULL THEN 'BAJA+2'
            ELSE NULL
        END AS clase_ternaria
    FROM clientes_foto_mes;
    """)

    con.execute("""
    CREATE OR REPLACE TABLE competencia_01 AS
    SELECT c.*, e.clase_ternaria
    FROM competencia_01_crudo c
    LEFT JOIN clientes_estado e
        ON c.numero_de_cliente = e.numero_de_cliente
        AND c.foto_mes = e.foto_mes_actual;
    """)

    con.execute("""
    CREATE OR REPLACE TABLE final AS
    WITH periodos AS (
        SELECT DISTINCT foto_mes FROM competencia_01_crudo
    ), clientes AS (
        SELECT DISTINCT numero_de_cliente FROM competencia_01_crudo
    ), todo AS (
        SELECT numero_de_cliente, foto_mes FROM clientes CROSS JOIN periodos
    ), clase_ternaria AS (
        SELECT
            t.numero_de_cliente,
            t.foto_mes,
            IF(c.numero_de_cliente IS NULL, 0, 1) AS mes_0,
            LEAD(mes_0, 1) OVER (PARTITION BY t.numero_de_cliente ORDER BY foto_mes) AS mes_1,
            LEAD(mes_0, 2) OVER (PARTITION BY t.numero_de_cliente ORDER BY foto_mes) AS mes_2
        FROM todo t
        LEFT JOIN competencia_01_crudo c USING (numero_de_cliente, foto_mes)
    )
    SELECT *
    EXCLUDE (mes_0, mes_1, mes_2)
    FROM clase_ternaria
    WHERE mes_0 = 1;
    """)

    con.execute("""
    PIVOT final
    ON clase_ternaria
    USING COUNT(numero_de_cliente)
    GROUP BY foto_mes;
    """)

    con.execute(f"""
    COPY final TO '{dataset_path}competencia_01.csv' (FORMAT CSV, HEADER);
    """)

# Ejemplo de uso
dataset_path = '/home/aleb/DMEyF/2024/datos/'
dataset_file = 'competencia_01_crudo.csv'
transformar_datos(dataset_path, dataset_file)
