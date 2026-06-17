import os
from datetime import date, datetime
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor
import clickhouse_connect


POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "zettelkasten"),
    "user": os.getenv("POSTGRES_USER", "zettel_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "zettel_pass"),
}

CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
    "username": os.getenv("CLICKHOUSE_USER", "zettel_user"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "zettel_pass"),
    "database": os.getenv("CLICKHOUSE_DB", "zettelkasten"),
}


def fetch_all(cur, query):
    cur.execute(query)
    return cur.fetchall()


def normalize_value(value):
    if value is None:
        return ""

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, datetime):
        # ClickHouse DateTime no necesita tzinfo en este esquema.
        return value.replace(tzinfo=None)

    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    return value


def rows_from_dicts(dict_rows, columns):
    return [
        [normalize_value(row[col]) for col in columns]
        for row in dict_rows
    ]


def truncate_tables(ch_client):
    tables = [
        "nota_analytics",
        "link_analytics",
        "tag_analytics",
        "evento_analytics",
        "benchmark_analytics",
    ]

    for table in tables:
        print(f"Limpiando {table}...")
        ch_client.command(f"TRUNCATE TABLE IF EXISTS zettelkasten.{table}")


def main():
    print("Conectando a PostgreSQL...")
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)

    print("Conectando a ClickHouse...")
    ch_client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

    try:
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            print("Leyendo nota_analytics...")
            nota_analytics = fetch_all(cur, """
                SELECT
                    n.id_nota,
                    n.titulo,
                    n.slug,
                    n.tipo,
                    n.estado,
                    COALESCE(u.nombre, '') AS autor,
                    COALESCE(m.nombre, '') AS materia,
                    COALESCE(t.nombre, '') AS tema,
                    COALESCE(tag_counts.total_tags, 0)::int AS total_tags,
                    COALESCE(out_links.total_links_salientes, 0)::int AS total_links_salientes,
                    COALESCE(in_links.total_backlinks, 0)::int AS total_backlinks,
                    n.fecha_creacion
                FROM nota n
                LEFT JOIN usuario u ON n.id_usuario = u.id_usuario
                LEFT JOIN tema t ON n.id_tema = t.id_tema
                LEFT JOIN materia m ON t.id_materia = m.id_materia
                LEFT JOIN (
                    SELECT id_nota, COUNT(*) AS total_tags
                    FROM nota_tag
                    GROUP BY id_nota
                ) tag_counts ON n.id_nota = tag_counts.id_nota
                LEFT JOIN (
                    SELECT id_nota_origen AS id_nota, COUNT(*) AS total_links_salientes
                    FROM nota_link
                    GROUP BY id_nota_origen
                ) out_links ON n.id_nota = out_links.id_nota
                LEFT JOIN (
                    SELECT id_nota_destino AS id_nota, COUNT(*) AS total_backlinks
                    FROM nota_link
                    GROUP BY id_nota_destino
                ) in_links ON n.id_nota = in_links.id_nota
                ORDER BY n.id_nota;
            """)

            print("Leyendo link_analytics...")
            link_analytics = fetch_all(cur, """
                SELECT
                    l.id_link,
                    origen.titulo AS nota_origen,
                    origen.slug AS slug_origen,
                    destino.titulo AS nota_destino,
                    destino.slug AS slug_destino,
                    l.tipo_link,
                    l.peso::float AS peso,
                    l.fecha_creacion
                FROM nota_link l
                JOIN nota origen ON l.id_nota_origen = origen.id_nota
                JOIN nota destino ON l.id_nota_destino = destino.id_nota
                ORDER BY l.id_link;
            """)

            print("Leyendo tag_analytics...")
            tag_analytics = fetch_all(cur, """
                SELECT
                    n.id_nota,
                    n.titulo,
                    n.slug,
                    tg.nombre AS tag,
                    COALESCE(m.nombre, '') AS materia,
                    COALESCE(t.nombre, '') AS tema,
                    n.fecha_creacion
                FROM nota_tag nt
                JOIN nota n ON nt.id_nota = n.id_nota
                JOIN tag tg ON nt.id_tag = tg.id_tag
                LEFT JOIN tema t ON n.id_tema = t.id_tema
                LEFT JOIN materia m ON t.id_materia = m.id_materia
                ORDER BY tg.nombre, n.id_nota;
            """)

            print("Leyendo evento_analytics...")
            evento_analytics = fetch_all(cur, """
                SELECT
                    e.id_evento,
                    COALESCE(e.id_nota, 0) AS id_nota,
                    COALESCE(n.titulo, '') AS titulo,
                    COALESCE(u.nombre, '') AS usuario,
                    e.tipo_evento,
                    e.fecha_evento
                FROM nota_evento e
                LEFT JOIN nota n ON e.id_nota = n.id_nota
                LEFT JOIN usuario u ON e.id_usuario = u.id_usuario
                ORDER BY e.id_evento;
            """)

            print("Leyendo benchmark_analytics...")
            benchmark_analytics = fetch_all(cur, """
                SELECT
                    id_consulta,
                    dbms,
                    tipo_consulta,
                    tiempo_ms::float AS tiempo_ms,
                    cantidad_resultados,
                    fecha_ejecucion
                FROM nota_consulta
                ORDER BY id_consulta;
            """)

        truncate_tables(ch_client)

        print("Insertando nota_analytics...")
        nota_columns = [
            "id_nota",
            "titulo",
            "slug",
            "tipo",
            "estado",
            "autor",
            "materia",
            "tema",
            "total_tags",
            "total_links_salientes",
            "total_backlinks",
            "fecha_creacion",
        ]
        ch_client.insert(
            "nota_analytics",
            rows_from_dicts(nota_analytics, nota_columns),
            column_names=nota_columns,
        )

        print("Insertando link_analytics...")
        link_columns = [
            "id_link",
            "nota_origen",
            "slug_origen",
            "nota_destino",
            "slug_destino",
            "tipo_link",
            "peso",
            "fecha_creacion",
        ]
        ch_client.insert(
            "link_analytics",
            rows_from_dicts(link_analytics, link_columns),
            column_names=link_columns,
        )

        print("Insertando tag_analytics...")
        tag_columns = [
            "id_nota",
            "titulo",
            "slug",
            "tag",
            "materia",
            "tema",
            "fecha_creacion",
        ]
        ch_client.insert(
            "tag_analytics",
            rows_from_dicts(tag_analytics, tag_columns),
            column_names=tag_columns,
        )

        print("Insertando evento_analytics...")
        evento_columns = [
            "id_evento",
            "id_nota",
            "titulo",
            "usuario",
            "tipo_evento",
            "fecha_evento",
        ]
        ch_client.insert(
            "evento_analytics",
            rows_from_dicts(evento_analytics, evento_columns),
            column_names=evento_columns,
        )

        if benchmark_analytics:
            print("Insertando benchmark_analytics...")
            benchmark_columns = [
                "id_consulta",
                "dbms",
                "tipo_consulta",
                "tiempo_ms",
                "cantidad_resultados",
                "fecha_ejecucion",
            ]
            ch_client.insert(
                "benchmark_analytics",
                rows_from_dicts(benchmark_analytics, benchmark_columns),
                column_names=benchmark_columns,
            )

        print("\nCarga PostgreSQL → ClickHouse terminada correctamente.")
        print(f"nota_analytics: {len(nota_analytics)}")
        print(f"link_analytics: {len(link_analytics)}")
        print(f"tag_analytics: {len(tag_analytics)}")
        print(f"evento_analytics: {len(evento_analytics)}")
        print(f"benchmark_analytics: {len(benchmark_analytics)}")

    finally:
        pg_conn.close()
        ch_client.close()


if __name__ == "__main__":
    main()
