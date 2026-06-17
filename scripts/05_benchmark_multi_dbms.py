import os
import time
from statistics import mean

import pandas as pd
import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
import clickhouse_connect
from tabulate import tabulate


RUNS = 8
WARMUP = 2


POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "zettelkasten"),
    "user": os.getenv("POSTGRES_USER", "zettel_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "zettel_pass"),
}

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:root_pass@localhost:27017/?authSource=admin"
)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "zettel_pass")

CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
    "username": os.getenv("CLICKHOUSE_USER", "zettel_user"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "zettel_pass"),
    "database": os.getenv("CLICKHOUSE_DB", "zettelkasten"),
}


def benchmark(dbms_name, query_name, func):
    for _ in range(WARMUP):
        func()

    times = []
    last_result = None

    for _ in range(RUNS):
        start = time.perf_counter()
        last_result = func()
        end = time.perf_counter()
        times.append((end - start) * 1000)

    return {
        "consulta": query_name,
        "dbms": dbms_name,
        "avg_ms": mean(times),
        "min_ms": min(times),
        "max_ms": max(times),
        "resultados": len(last_result) if last_result is not None else 0,
    }


def pg_query(sql):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def mongo_query(pipeline):
    client = MongoClient(MONGO_URI)
    try:
        db = client["zettelkasten"]
        return list(db.notas.aggregate(pipeline))
    finally:
        client.close()


def neo4j_query(cypher):
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )
    try:
        with driver.session() as session:
            return list(session.run(cypher))
    finally:
        driver.close()


def clickhouse_query(sql):
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    try:
        result = client.query(sql)
        return result.result_rows
    finally:
        client.close()


# ============================================================
# CONSULTA 1: agregación simple por estado
# Punto fuerte esperado: ClickHouse
# ============================================================

def q1_postgres():
    return pg_query("""
        SELECT estado, COUNT(*) AS total
        FROM nota
        GROUP BY estado
        ORDER BY total DESC;
    """)


def q1_mongo():
    return mongo_query([
        {"$group": {"_id": "$estado", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}}
    ])


def q1_neo4j():
    return neo4j_query("""
        MATCH (n:Nota)
        RETURN n.estado AS estado, count(n) AS total
        ORDER BY total DESC
    """)


def q1_clickhouse():
    return clickhouse_query("""
        SELECT estado, count(*) AS total
        FROM zettelkasten.nota_analytics
        GROUP BY estado
        ORDER BY total DESC
    """)


# ============================================================
# CONSULTA 2: agregación temporal por año y mes
# Punto fuerte esperado: ClickHouse
# ============================================================

def q2_postgres():
    return pg_query("""
        SELECT 
            EXTRACT(YEAR FROM fecha_creacion)::int AS anio,
            EXTRACT(MONTH FROM fecha_creacion)::int AS mes,
            COUNT(*) AS total
        FROM nota
        GROUP BY anio, mes
        ORDER BY anio, mes;
    """)


def q2_mongo():
    return mongo_query([
        {
            "$addFields": {
                "fecha_creacion_date": {"$toDate": "$fechas.creacion"}
            }
        },
        {
            "$group": {
                "_id": {
                    "anio": {"$year": "$fecha_creacion_date"},
                    "mes": {"$month": "$fecha_creacion_date"}
                },
                "total": {"$sum": 1}
            }
        },
        {"$sort": {"_id.anio": 1, "_id.mes": 1}}
    ])


def q2_neo4j():
    return neo4j_query("""
        MATCH (n:Nota)
        WITH datetime(n.fecha_creacion) AS fecha
        RETURN fecha.year AS anio, fecha.month AS mes, count(*) AS total
        ORDER BY anio, mes
    """)


def q2_clickhouse():
    return clickhouse_query("""
        SELECT 
            toYear(fecha_creacion) AS anio,
            toMonth(fecha_creacion) AS mes,
            count(*) AS total
        FROM zettelkasten.nota_analytics
        GROUP BY anio, mes
        ORDER BY anio, mes
    """)


# ============================================================
# CONSULTA 3: consulta documental con datos embebidos
# Punto fuerte esperado: MongoDB
# ============================================================

def q3_postgres():
    return pg_query("""
        SELECT
            n.titulo,
            n.estado,
            t.nombre AS tema,
            COUNT(DISTINCT nt.id_tag) AS total_tags,
            COUNT(DISTINCT incoming.id_link) AS total_backlinks
        FROM nota n
        JOIN tema t ON n.id_tema = t.id_tema
        LEFT JOIN nota_tag nt ON n.id_nota = nt.id_nota
        LEFT JOIN nota_link incoming ON n.id_nota = incoming.id_nota_destino
        WHERE n.estado = 'activa'
        GROUP BY n.id_nota, n.titulo, n.estado, t.nombre
        HAVING COUNT(DISTINCT nt.id_tag) >= 3
           AND COUNT(DISTINCT incoming.id_link) >= 2
        ORDER BY total_backlinks DESC, total_tags DESC
        LIMIT 25;
    """)


def q3_mongo():
    return mongo_query([
        {
            "$match": {
                "estado": "activa",
                "metricas.total_tags": {"$gte": 3},
                "metricas.total_backlinks": {"$gte": 2}
            }
        },
        {
            "$project": {
                "_id": 0,
                "titulo": 1,
                "estado": 1,
                "tema": "$materia.tema.nombre",
                "total_tags": "$metricas.total_tags",
                "total_backlinks": "$metricas.total_backlinks"
            }
        },
        {
            "$sort": {
                "total_backlinks": -1,
                "total_tags": -1
            }
        },
        {"$limit": 25}
    ])


def q3_neo4j():
    return neo4j_query("""
        MATCH (n:Nota)-[:PERTENECE_A]->(t:Tema)
        WHERE n.estado = 'activa'
        OPTIONAL MATCH (n)-[:TIENE_TAG]->(tag:Tag)
        WITH n, t, count(DISTINCT tag) AS total_tags
        OPTIONAL MATCH (incoming:Nota)-[:LINK]->(n)
        WITH n, t, total_tags, count(DISTINCT incoming) AS total_backlinks
        WHERE total_tags >= 3 AND total_backlinks >= 2
        RETURN 
            n.titulo AS titulo,
            n.estado AS estado,
            t.nombre AS tema,
            total_tags,
            total_backlinks
        ORDER BY total_backlinks DESC, total_tags DESC
        LIMIT 25
    """)


def q3_clickhouse():
    return clickhouse_query("""
        SELECT
            titulo,
            estado,
            tema,
            total_tags,
            total_backlinks
        FROM zettelkasten.nota_analytics
        WHERE estado = 'activa'
          AND total_tags >= 3
          AND total_backlinks >= 2
        ORDER BY total_backlinks DESC, total_tags DESC
        LIMIT 25
    """)


# ============================================================
# CONSULTA 4: recorrido de grafo a 2 saltos
# Punto fuerte esperado: Neo4j
# ============================================================

def q4_postgres():
    return pg_query("""
        SELECT DISTINCT
            n0.slug AS origen,
            n2.slug AS destino_2_saltos
        FROM nota n0
        JOIN nota_link l1 ON n0.id_nota = l1.id_nota_origen
        JOIN nota n1 ON l1.id_nota_destino = n1.id_nota
        JOIN nota_link l2 ON n1.id_nota = l2.id_nota_origen
        JOIN nota n2 ON l2.id_nota_destino = n2.id_nota
        WHERE n0.slug = 'nota-1-algebra-lineal'
          AND n2.slug <> n0.slug
        LIMIT 100;
    """)


def q4_mongo():
    client = MongoClient(MONGO_URI)
    try:
        db = client["zettelkasten"]

        start = db.notas.find_one(
            {"slug": "nota-1-algebra-lineal"},
            {"links.nota.slug": 1, "_id": 0}
        )

        if not start:
            return []

        first_hop_slugs = [
            link["nota"]["slug"]
            for link in start.get("links", [])
            if "nota" in link and "slug" in link["nota"]
        ]

        second_hop_docs = list(db.notas.find(
            {"slug": {"$in": first_hop_slugs}},
            {"slug": 1, "links.nota.slug": 1, "_id": 0}
        ))

        results = set()

        for doc in second_hop_docs:
            for link in doc.get("links", []):
                destino = link.get("nota", {}).get("slug")
                if destino and destino != "nota-1-algebra-lineal":
                    results.add(("nota-1-algebra-lineal", destino))

        return list(results)[:100]

    finally:
        client.close()


def q4_neo4j():
    return neo4j_query("""
        MATCH (origen:Nota {slug: 'nota-1-algebra-lineal'})-[:LINK]->(:Nota)-[:LINK]->(destino:Nota)
        WHERE destino.slug <> origen.slug
        RETURN DISTINCT origen.slug AS origen, destino.slug AS destino_2_saltos
        LIMIT 100
    """)


def q4_clickhouse():
    return clickhouse_query("""
        SELECT DISTINCT
            l1.slug_origen AS origen,
            l2.slug_destino AS destino_2_saltos
        FROM zettelkasten.link_analytics l1
        INNER JOIN zettelkasten.link_analytics l2
            ON l1.slug_destino = l2.slug_origen
        WHERE l1.slug_origen = 'nota-1-algebra-lineal'
          AND l2.slug_destino != l1.slug_origen
        LIMIT 100
    """)


# ============================================================
# CONSULTA 5: ranking complejo con tags, links y eventos
# Punto fuerte esperado: PostgreSQL/ClickHouse
# ============================================================

def q5_postgres():
    return pg_query("""
        SELECT
            n.titulo,
            t.nombre AS tema,
            m.nombre AS materia,
            COUNT(DISTINCT nt.id_tag) AS total_tags,
            COUNT(DISTINCT out_l.id_link) AS total_links_salientes,
            COUNT(DISTINCT in_l.id_link) AS total_backlinks,
            COUNT(DISTINCT e.id_evento) AS total_eventos
        FROM nota n
        JOIN tema t ON n.id_tema = t.id_tema
        JOIN materia m ON t.id_materia = m.id_materia
        LEFT JOIN nota_tag nt ON n.id_nota = nt.id_nota
        LEFT JOIN nota_link out_l ON n.id_nota = out_l.id_nota_origen
        LEFT JOIN nota_link in_l ON n.id_nota = in_l.id_nota_destino
        LEFT JOIN nota_evento e ON n.id_nota = e.id_nota
        GROUP BY n.id_nota, n.titulo, t.nombre, m.nombre
        ORDER BY 
            total_eventos DESC,
            total_backlinks DESC,
            total_links_salientes DESC
        LIMIT 25;
    """)


def q5_mongo():
    return mongo_query([
        {
            "$project": {
                "_id": 0,
                "titulo": 1,
                "tema": "$materia.tema.nombre",
                "materia": "$materia.nombre",
                "total_tags": "$metricas.total_tags",
                "total_links_salientes": "$metricas.total_links_salientes",
                "total_backlinks": "$metricas.total_backlinks",
                "total_eventos": "$metricas.total_eventos"
            }
        },
        {
            "$sort": {
                "total_eventos": -1,
                "total_backlinks": -1,
                "total_links_salientes": -1
            }
        },
        {"$limit": 25}
    ])


def q5_neo4j():
    return neo4j_query("""
        MATCH (n:Nota)-[:PERTENECE_A]->(t:Tema)-[:ES_PARTE_DE]->(m:Materia)
        OPTIONAL MATCH (n)-[:TIENE_TAG]->(tag:Tag)
        WITH n, t, m, count(DISTINCT tag) AS total_tags
        OPTIONAL MATCH (n)-[:LINK]->(out:Nota)
        WITH n, t, m, total_tags, count(DISTINCT out) AS total_links_salientes
        OPTIONAL MATCH (in:Nota)-[:LINK]->(n)
        WITH n, t, m, total_tags, total_links_salientes, count(DISTINCT in) AS total_backlinks
        RETURN
            n.titulo AS titulo,
            t.nombre AS tema,
            m.nombre AS materia,
            total_tags,
            total_links_salientes,
            total_backlinks,
            0 AS total_eventos
        ORDER BY total_backlinks DESC, total_links_salientes DESC
        LIMIT 25
    """)


def q5_clickhouse():
    return clickhouse_query("""
        SELECT
            titulo,
            tema,
            materia,
            total_tags,
            total_links_salientes,
            total_backlinks,
            0 AS total_eventos
        FROM zettelkasten.nota_analytics
        ORDER BY total_backlinks DESC, total_links_salientes DESC, total_tags DESC
        LIMIT 25
    """)


QUERIES = [
    {
        "name": "Q1_agregacion_estado",
        "expected": "ClickHouse",
        "funcs": {
            "PostgreSQL": q1_postgres,
            "MongoDB": q1_mongo,
            "Neo4j": q1_neo4j,
            "ClickHouse": q1_clickhouse,
        },
    },
    {
        "name": "Q2_agregacion_temporal",
        "expected": "ClickHouse",
        "funcs": {
            "PostgreSQL": q2_postgres,
            "MongoDB": q2_mongo,
            "Neo4j": q2_neo4j,
            "ClickHouse": q2_clickhouse,
        },
    },
    {
        "name": "Q3_documental_embebido",
        "expected": "MongoDB",
        "funcs": {
            "PostgreSQL": q3_postgres,
            "MongoDB": q3_mongo,
            "Neo4j": q3_neo4j,
            "ClickHouse": q3_clickhouse,
        },
    },
    {
        "name": "Q4_recorrido_grafo_2_saltos",
        "expected": "Neo4j",
        "funcs": {
            "PostgreSQL": q4_postgres,
            "MongoDB": q4_mongo,
            "Neo4j": q4_neo4j,
            "ClickHouse": q4_clickhouse,
        },
    },
    {
        "name": "Q5_ranking_complejo",
        "expected": "PostgreSQL/ClickHouse",
        "funcs": {
            "PostgreSQL": q5_postgres,
            "MongoDB": q5_mongo,
            "Neo4j": q5_neo4j,
            "ClickHouse": q5_clickhouse,
        },
    },
]


def main():
    all_results = []

    for query_def in QUERIES:
        query_name = query_def["name"]
        expected = query_def["expected"]

        print("\n" + "=" * 90)
        print(f"Consulta: {query_name}")
        print(f"DBMS esperado fuerte: {expected}")
        print("=" * 90)

        query_results = []

        for dbms_name, func in query_def["funcs"].items():
            try:
                result = benchmark(dbms_name, query_name, func)
                query_results.append(result)
                all_results.append(result)
            except Exception as e:
                error_row = {
                    "consulta": query_name,
                    "dbms": dbms_name,
                    "avg_ms": None,
                    "min_ms": None,
                    "max_ms": None,
                    "resultados": None,
                    "error": str(e),
                }
                query_results.append(error_row)
                all_results.append(error_row)

        printable = []
        valid_results = []

        for row in query_results:
            if row.get("avg_ms") is None:
                printable.append([
                    row["dbms"],
                    "ERROR",
                    "ERROR",
                    "ERROR",
                    row.get("resultados"),
                    row.get("error")[:80],
                ])
            else:
                valid_results.append(row)
                printable.append([
                    row["dbms"],
                    round(row["avg_ms"], 3),
                    round(row["min_ms"], 3),
                    round(row["max_ms"], 3),
                    row["resultados"],
                    "",
                ])

        print(tabulate(
            printable,
            headers=["DBMS", "avg_ms", "min_ms", "max_ms", "resultados", "error"],
            tablefmt="github"
        ))

        if valid_results:
            winner = min(valid_results, key=lambda r: r["avg_ms"])
            print(f"\nGanador medido: {winner['dbms']} ({winner['avg_ms']:.3f} ms)")
            print(f"Ganador esperado/conceptual: {expected}")

    df = pd.DataFrame(all_results)
    df.to_csv("benchmark_multi_dbms.csv", index=False)

    print("\n" + "=" * 90)
    print("Benchmark terminado.")
    print("Archivo generado: benchmark_multi_dbms.csv")
    print("=" * 90)


if __name__ == "__main__":
    main()
