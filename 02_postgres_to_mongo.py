import os
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient


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

MONGO_DB = os.getenv("MONGO_DB", "zettelkasten")


def to_jsonable(value):
    if isinstance(value, dict):
        return {key: to_jsonable(val) for key, val in value.items()}

    if isinstance(value, list):
        return [to_jsonable(item) for item in value]

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    return value


def fetch_all(cur, query):
    cur.execute(query)
    return cur.fetchall()


def main():
    print("Conectando a PostgreSQL...")
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)

    print("Conectando a MongoDB...")
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    notas_collection = mongo_db["notas"]
    benchmarks_collection = mongo_db["benchmarks"]

    try:
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            print("Leyendo notas base...")
            notas = fetch_all(cur, """
                SELECT
                    n.id_nota,
                    n.titulo,
                    n.slug,
                    n.contenido,
                    n.tipo,
                    n.estado,
                    n.fecha_creacion,
                    n.fecha_actualizacion,

                    u.nombre AS autor_nombre,
                    u.email AS autor_email,
                    u.rol AS autor_rol,

                    t.nombre AS tema_nombre,
                    t.descripcion AS tema_descripcion,

                    m.nombre AS materia_nombre,
                    m.descripcion AS materia_descripcion
                FROM nota n
                LEFT JOIN usuario u ON n.id_usuario = u.id_usuario
                LEFT JOIN tema t ON n.id_tema = t.id_tema
                LEFT JOIN materia m ON t.id_materia = m.id_materia
                ORDER BY n.id_nota;
            """)

            print("Leyendo tags...")
            tags_rows = fetch_all(cur, """
                SELECT
                    nt.id_nota,
                    tg.nombre,
                    tg.descripcion
                FROM nota_tag nt
                JOIN tag tg ON nt.id_tag = tg.id_tag
                ORDER BY nt.id_nota, tg.nombre;
            """)

            print("Leyendo fuentes...")
            fuentes_rows = fetch_all(cur, """
                SELECT
                    nf.id_nota,
                    f.tipo,
                    f.titulo,
                    f.autor,
                    f.url,
                    f.anio,
                    nf.cita,
                    nf.pagina
                FROM nota_fuente nf
                JOIN fuente f ON nf.id_fuente = f.id_fuente
                ORDER BY nf.id_nota, f.titulo;
            """)

            print("Leyendo links y backlinks...")
            links_rows = fetch_all(cur, """
                SELECT
                    l.id_nota_origen,
                    l.id_nota_destino,
                    l.tipo_link,
                    l.contexto,
                    l.peso,
                    l.fecha_creacion,

                    origen.titulo AS origen_titulo,
                    origen.slug AS origen_slug,
                    origen.tipo AS origen_tipo,
                    origen.estado AS origen_estado,

                    destino.titulo AS destino_titulo,
                    destino.slug AS destino_slug,
                    destino.tipo AS destino_tipo,
                    destino.estado AS destino_estado
                FROM nota_link l
                JOIN nota origen ON l.id_nota_origen = origen.id_nota
                JOIN nota destino ON l.id_nota_destino = destino.id_nota
                ORDER BY l.id_nota_origen, l.fecha_creacion;
            """)

            print("Leyendo versiones...")
            versiones_rows = fetch_all(cur, """
                SELECT
                    id_nota,
                    numero_version,
                    contenido_anterior,
                    contenido_nuevo,
                    comentario,
                    fecha_version
                FROM nota_version
                ORDER BY id_nota, numero_version;
            """)

            print("Leyendo eventos...")
            eventos_rows = fetch_all(cur, """
                SELECT
                    e.id_nota,
                    e.tipo_evento,
                    e.fecha_evento,
                    e.metadata,
                    u.nombre AS usuario_nombre,
                    u.rol AS usuario_rol
                FROM nota_evento e
                LEFT JOIN usuario u ON e.id_usuario = u.id_usuario
                ORDER BY e.id_nota, e.fecha_evento;
            """)

        print("Agrupando datos para documentos JSON...")

        tags_by_note = defaultdict(list)
        for row in tags_rows:
            tags_by_note[row["id_nota"]].append({
                "nombre": row["nombre"],
                "descripcion": row["descripcion"],
            })

        fuentes_by_note = defaultdict(list)
        for row in fuentes_rows:
            fuentes_by_note[row["id_nota"]].append({
                "tipo": row["tipo"],
                "titulo": row["titulo"],
                "autor": row["autor"],
                "url": row["url"],
                "anio": row["anio"],
                "referencia_en_nota": {
                    "cita": row["cita"],
                    "pagina": row["pagina"],
                },
            })

        links_by_origin = defaultdict(list)
        backlinks_by_destination = defaultdict(list)

        for row in links_rows:
            outgoing_link = {
                "tipo": row["tipo_link"],
                "contexto": row["contexto"],
                "peso": row["peso"],
                "fecha_creacion": row["fecha_creacion"],
                "nota": {
                    "titulo": row["destino_titulo"],
                    "slug": row["destino_slug"],
                    "tipo": row["destino_tipo"],
                    "estado": row["destino_estado"],
                },
            }

            backlink = {
                "tipo": row["tipo_link"],
                "contexto": row["contexto"],
                "peso": row["peso"],
                "fecha_creacion": row["fecha_creacion"],
                "nota": {
                    "titulo": row["origen_titulo"],
                    "slug": row["origen_slug"],
                    "tipo": row["origen_tipo"],
                    "estado": row["origen_estado"],
                },
            }

            links_by_origin[row["id_nota_origen"]].append(outgoing_link)
            backlinks_by_destination[row["id_nota_destino"]].append(backlink)

        versiones_by_note = defaultdict(list)
        for row in versiones_rows:
            versiones_by_note[row["id_nota"]].append({
                "numero_version": row["numero_version"],
                "contenido_anterior": row["contenido_anterior"],
                "contenido_nuevo": row["contenido_nuevo"],
                "comentario": row["comentario"],
                "fecha_version": row["fecha_version"],
            })

        eventos_by_note = defaultdict(list)
        for row in eventos_rows:
            eventos_by_note[row["id_nota"]].append({
                "tipo": row["tipo_evento"],
                "fecha": row["fecha_evento"],
                "usuario": {
                    "nombre": row["usuario_nombre"],
                    "rol": row["usuario_rol"],
                },
                "metadata": row["metadata"],
            })

        print("Construyendo documentos MongoDB...")

        documentos = []

        for nota in notas:
            id_nota = nota["id_nota"]

            links = links_by_origin.get(id_nota, [])
            backlinks = backlinks_by_destination.get(id_nota, [])
            tags = tags_by_note.get(id_nota, [])
            fuentes = fuentes_by_note.get(id_nota, [])
            versiones = versiones_by_note.get(id_nota, [])
            eventos = eventos_by_note.get(id_nota, [])

            documento = {
                "titulo": nota["titulo"],
                "slug": nota["slug"],
                "contenido": nota["contenido"],
                "tipo": nota["tipo"],
                "estado": nota["estado"],
                "fechas": {
                    "creacion": nota["fecha_creacion"],
                    "actualizacion": nota["fecha_actualizacion"],
                },
                "autor": {
                    "nombre": nota["autor_nombre"],
                    "email": nota["autor_email"],
                    "rol": nota["autor_rol"],
                },
                "materia": {
                    "nombre": nota["materia_nombre"],
                    "descripcion": nota["materia_descripcion"],
                    "tema": {
                        "nombre": nota["tema_nombre"],
                        "descripcion": nota["tema_descripcion"],
                    },
                },
                "tags": tags,
                "fuentes": fuentes,
                "links": links,
                "backlinks": backlinks,
                "versiones": versiones,
                "eventos": eventos,
                "metricas": {
                    "total_tags": len(tags),
                    "total_fuentes": len(fuentes),
                    "total_links_salientes": len(links),
                    "total_backlinks": len(backlinks),
                    "total_versiones": len(versiones),
                    "total_eventos": len(eventos),
                },
            }

            documentos.append(to_jsonable(documento))

        print("Limpiando colección notas...")
        notas_collection.delete_many({})

        print("Insertando documentos en MongoDB...")
        if documentos:
            notas_collection.insert_many(documentos, ordered=False)

        print("Creando índices...")
        notas_collection.create_index("slug", unique=True)
        notas_collection.create_index("titulo")
        notas_collection.create_index("tipo")
        notas_collection.create_index("estado")
        notas_collection.create_index("autor.nombre")
        notas_collection.create_index("materia.nombre")
        notas_collection.create_index("materia.tema.nombre")
        notas_collection.create_index("tags.nombre")
        notas_collection.create_index("links.nota.slug")
        notas_collection.create_index("backlinks.nota.slug")
        notas_collection.create_index("metricas.total_backlinks")
        benchmarks_collection.create_index([("dbms", 1), ("tipo_consulta", 1)])

        total_mongo = notas_collection.count_documents({})

        print("\nTransformación PostgreSQL → MongoDB terminada correctamente.")
        print(f"Notas leídas desde PostgreSQL: {len(notas)}")
        print(f"Documentos insertados en MongoDB: {total_mongo}")

        ejemplo = notas_collection.find_one(
            {},
            {
                "_id": 0,
                "titulo": 1,
                "slug": 1,
                "autor": 1,
                "materia": 1,
                "tags": {"$slice": 3},
                "links": {"$slice": 2},
                "backlinks": {"$slice": 2},
                "metricas": 1,
            },
        )

        print("\nEjemplo de documento:")
        print(ejemplo)

    finally:
        pg_conn.close()
        mongo_client.close()


if __name__ == "__main__":
    main()
