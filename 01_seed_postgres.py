import os
import random
import re
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values, Json


DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "zettelkasten"),
    "user": os.getenv("POSTGRES_USER", "zettel_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "zettel_pass"),
}

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# Puedes subir estos números después para benchmarks más pesados.
NUM_USUARIOS = 25
NUM_NOTAS = 1200
NUM_TAGS = 80
NUM_FUENTES = 250
NUM_LINKS = 3000
NUM_EVENTOS = 5000
MAX_VERSIONES_POR_NOTA = 4


MATERIAS = [
    "Bases de Datos",
    "Inteligencia Artificial",
    "Matemáticas",
    "Sistemas Distribuidos",
    "Filosofía",
    "Programación",
    "Ciencia de Datos",
    "Arquitectura de Software",
]

TEMAS_POR_MATERIA = {
    "Bases de Datos": [
        "Modelo Relacional",
        "SQL",
        "NoSQL",
        "MongoDB",
        "Neo4j",
        "ClickHouse",
        "Índices",
        "Transacciones",
        "Normalización",
        "OLAP",
    ],
    "Inteligencia Artificial": [
        "Machine Learning",
        "Deep Learning",
        "NLP",
        "Embeddings",
        "Redes Neuronales",
        "Agentes",
    ],
    "Matemáticas": [
        "Álgebra Lineal",
        "Probabilidad",
        "Estadística",
        "Optimización",
        "Cálculo",
    ],
    "Sistemas Distribuidos": [
        "Consistencia",
        "Replicación",
        "Particionamiento",
        "Consenso",
        "Escalabilidad",
    ],
    "Filosofía": [
        "Epistemología",
        "Lógica",
        "Filosofía de la Ciencia",
        "Ontología",
    ],
    "Programación": [
        "Python",
        "Estructuras de Datos",
        "Algoritmos",
        "Backend",
        "APIs",
    ],
    "Ciencia de Datos": [
        "Limpieza de Datos",
        "Visualización",
        "Pipelines",
        "Análisis Exploratorio",
    ],
    "Arquitectura de Software": [
        "Microservicios",
        "Diseño de Sistemas",
        "Patrones",
        "Eventos",
    ],
}

TIPOS_NOTA = ["concepto", "clase", "resumen", "idea", "pregunta", "paper", "ejemplo"]
ESTADOS = ["activa", "activa", "activa", "borrador", "archivada"]

TIPOS_LINK = [
    "referencia",
    "expande",
    "contradice",
    "ejemplo_de",
    "prerrequisito_de",
    "relacionado",
    "inspirado_en",
]

TIPOS_FUENTE = ["libro", "paper", "video", "clase", "documentacion", "articulo", "otro"]

TIPOS_EVENTO = [
    "creada",
    "editada",
    "consultada",
    "link_agregado",
    "tag_agregado",
    "fuente_agregada",
    "archivada",
]

PALABRAS = [
    "modelo",
    "documental",
    "grafo",
    "relacional",
    "consulta",
    "índice",
    "nota",
    "enlace",
    "backlink",
    "conocimiento",
    "tema",
    "fuente",
    "tag",
    "versión",
    "evento",
    "transacción",
    "agregación",
    "columna",
    "documento",
    "rendimiento",
    "normalización",
    "desnormalización",
    "anidamiento",
    "semántica",
    "obsidian",
    "zettelkasten",
]


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("á", "a").replace("é", "e").replace("í", "i")
    text = text.replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


DATA_START = datetime(2024, 1, 1, 0, 0, 0)
DATA_END = datetime(2026, 5, 31, 23, 59, 59)


def random_date(*args, **kwargs):
    """
    Genera fechas sintéticas entre enero de 2024 y mayo de 2026.
    Los argumentos se ignoran para mantener compatibilidad con llamadas antiguas
    como random_date(600), random_date(400), etc.
    """
    total_seconds = int((DATA_END - DATA_START).total_seconds())
    random_seconds = random.randint(0, total_seconds)
    return DATA_START + timedelta(seconds=random_seconds)


def bounded_future_date(start_date, max_days=120):
    """
    Genera una fecha posterior a start_date, pero nunca después de DATA_END.
    Sirve para fecha_actualizacion.
    """
    candidate = start_date + timedelta(
        days=random.randint(0, max_days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return min(candidate, DATA_END)


def random_sentence(min_words=8, max_words=18):
    words = random.choices(PALABRAS, k=random.randint(min_words, max_words))
    sentence = " ".join(words)
    return sentence.capitalize() + "."


def random_paragraph(sentences=4):
    return " ".join(random_sentence() for _ in range(sentences))


def reset_database(cur):
    print("Limpiando tablas existentes...")
    cur.execute("""
        TRUNCATE TABLE
            nota_consulta,
            nota_evento,
            nota_version,
            nota_link,
            nota_fuente,
            nota_tag,
            fuente,
            tag,
            nota,
            tema,
            materia,
            usuario
        RESTART IDENTITY CASCADE;
    """)


def insert_and_return_ids(cur, table_name, columns, rows, id_column):
    query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        RETURNING {id_column};
    """
    result = execute_values(cur, query, rows, page_size=1000, fetch=True)
    return [row[0] for row in result]


def main():
    print("Conectando a PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            reset_database(cur)

            print("Insertando usuarios...")
            usuarios = [
                (
                    f"Usuario {i}",
                    f"usuario{i}@zettel.local",
                    random.choice(["estudiante", "profesor", "investigador", "admin"]),
                    random_date(600),
                )
                for i in range(1, NUM_USUARIOS + 1)
            ]
            usuario_ids = insert_and_return_ids(
                cur,
                "usuario",
                ["nombre", "email", "rol", "fecha_registro"],
                usuarios,
                "id_usuario",
            )

            print("Insertando materias...")
            materias = [(m, f"Materia relacionada con {m}.") for m in MATERIAS]
            materia_ids = insert_and_return_ids(
                cur,
                "materia",
                ["nombre", "descripcion"],
                materias,
                "id_materia",
            )
            materia_id_by_name = dict(zip(MATERIAS, materia_ids))

            print("Insertando temas...")
            temas = []
            tema_nombre_by_id = {}
            tema_materia_by_id = {}

            for materia, temas_lista in TEMAS_POR_MATERIA.items():
                for tema in temas_lista:
                    temas.append(
                        (
                            materia_id_by_name[materia],
                            tema,
                            f"Tema {tema} dentro de {materia}.",
                        )
                    )

            tema_ids = insert_and_return_ids(
                cur,
                "tema",
                ["id_materia", "nombre", "descripcion"],
                temas,
                "id_tema",
            )

            for id_tema, row in zip(tema_ids, temas):
                id_materia, nombre_tema, _ = row
                tema_nombre_by_id[id_tema] = nombre_tema
                tema_materia_by_id[id_tema] = id_materia

            print("Insertando tags...")
            base_tags = [
                "sql",
                "postgresql",
                "mongodb",
                "json",
                "neo4j",
                "clickhouse",
                "columnar",
                "graph",
                "backlinks",
                "obsidian",
                "zettelkasten",
                "performance",
                "join",
                "documentos",
                "olap",
                "normalizacion",
                "indices",
                "agregacion",
                "python",
                "docker",
            ]

            extra_tags = [f"tag_{i}" for i in range(1, NUM_TAGS - len(base_tags) + 1)]
            tag_names = base_tags + extra_tags

            tags = [(tag, f"Etiqueta relacionada con {tag}.") for tag in tag_names]
            tag_ids = insert_and_return_ids(
                cur,
                "tag",
                ["nombre", "descripcion"],
                tags,
                "id_tag",
            )

            print("Insertando fuentes...")
            fuentes = []
            for i in range(1, NUM_FUENTES + 1):
                tipo = random.choice(TIPOS_FUENTE)
                fuentes.append(
                    (
                        tipo,
                        f"Fuente {i}: {random.choice(PALABRAS).capitalize()} y {random.choice(PALABRAS)}",
                        f"Autor {random.randint(1, 60)}",
                        f"https://example.com/fuente-{i}",
                        random.randint(1995, 2026),
                    )
                )

            fuente_ids = insert_and_return_ids(
                cur,
                "fuente",
                ["tipo", "titulo", "autor", "url", "anio"],
                fuentes,
                "id_fuente",
            )

            print("Insertando notas...")
            notas = []
            used_slugs = set()

            for i in range(1, NUM_NOTAS + 1):
                id_tema = random.choice(tema_ids)
                tema_nombre = tema_nombre_by_id[id_tema]
                titulo = f"{random.choice(['Nota', 'Idea', 'Concepto', 'Resumen', 'Pregunta'])} {i}: {tema_nombre}"
                base_slug = slugify(titulo)
                slug = base_slug
                suffix = 2

                while slug in used_slugs:
                    slug = f"{base_slug}-{suffix}"
                    suffix += 1

                used_slugs.add(slug)

                fecha_creacion = random_date(500)
                fecha_actualizacion = bounded_future_date(fecha_creacion, 120)

                contenido = (
                    f"# {titulo}\n\n"
                    f"{random_paragraph(5)}\n\n"
                    f"Esta nota forma parte de un sistema Zettelkasten con enlaces, backlinks, tags y fuentes."
                )

                notas.append(
                    (
                        random.choice(usuario_ids),
                        id_tema,
                        titulo,
                        slug,
                        contenido,
                        random.choice(TIPOS_NOTA),
                        random.choice(ESTADOS),
                        fecha_creacion,
                        fecha_actualizacion,
                    )
                )

            nota_ids = insert_and_return_ids(
                cur,
                "nota",
                [
                    "id_usuario",
                    "id_tema",
                    "titulo",
                    "slug",
                    "contenido",
                    "tipo",
                    "estado",
                    "fecha_creacion",
                    "fecha_actualizacion",
                ],
                notas,
                "id_nota",
            )

            print("Insertando nota_tag...")
            nota_tag_pairs = set()
            for id_nota in nota_ids:
                for id_tag in random.sample(tag_ids, random.randint(2, 7)):
                    nota_tag_pairs.add((id_nota, id_tag, random_date(400)))

            execute_values(
                cur,
                """
                INSERT INTO nota_tag (id_nota, id_tag, fecha_asignacion)
                VALUES %s;
                """,
                list(nota_tag_pairs),
            )

            print("Insertando nota_fuente...")
            nota_fuente_pairs = set()
            for id_nota in nota_ids:
                for id_fuente in random.sample(fuente_ids, random.randint(1, 3)):
                    nota_fuente_pairs.add(
                        (
                            id_nota,
                            id_fuente,
                            random_sentence(10, 20),
                            str(random.randint(1, 500)),
                        )
                    )

            execute_values(
                cur,
                """
                INSERT INTO nota_fuente (id_nota, id_fuente, cita, pagina)
                VALUES %s;
                """,
                list(nota_fuente_pairs),
            )

            print("Insertando nota_link para links y backlinks...")
            link_rows = []
            used_links = set()
            max_possible_attempts = NUM_LINKS * 10
            attempts = 0

            while len(link_rows) < NUM_LINKS and attempts < max_possible_attempts:
                attempts += 1
                origen = random.choice(nota_ids)
                destino = random.choice(nota_ids)

                if origen == destino:
                    continue

                tipo_link = random.choice(TIPOS_LINK)
                key = (origen, destino, tipo_link)

                if key in used_links:
                    continue

                used_links.add(key)
                link_rows.append(
                    (
                        origen,
                        destino,
                        tipo_link,
                        random_sentence(8, 16),
                        Decimal(str(round(random.uniform(0.10, 1.00), 2))),
                        random_date(350),
                    )
                )

            link_ids = insert_and_return_ids(
                cur,
                "nota_link",
                [
                    "id_nota_origen",
                    "id_nota_destino",
                    "tipo_link",
                    "contexto",
                    "peso",
                    "fecha_creacion",
                ],
                link_rows,
                "id_link",
            )

            print("Insertando versiones...")
            version_rows = []
            for id_nota in nota_ids:
                versiones = random.randint(1, MAX_VERSIONES_POR_NOTA)
                contenido_anterior = None

                for version in range(1, versiones + 1):
                    contenido_nuevo = random_paragraph(3)

                    version_rows.append(
                        (
                            id_nota,
                            version,
                            contenido_anterior,
                            contenido_nuevo,
                            f"Versión {version} de la nota.",
                            random_date(300),
                        )
                    )

                    contenido_anterior = contenido_nuevo

            execute_values(
                cur,
                """
                INSERT INTO nota_version (
                    id_nota,
                    numero_version,
                    contenido_anterior,
                    contenido_nuevo,
                    comentario,
                    fecha_version
                )
                VALUES %s;
                """,
                version_rows,
            )

            print("Insertando eventos...")
            evento_rows = []
            for _ in range(NUM_EVENTOS):
                id_nota = random.choice(nota_ids)
                id_usuario = random.choice(usuario_ids)
                tipo_evento = random.choice(TIPOS_EVENTO)

                metadata = {
                    "origen": random.choice(["web", "script", "importacion", "admin"]),
                    "ip_simulada": f"192.168.1.{random.randint(2, 254)}",
                    "detalle": random_sentence(5, 12),
                }

                evento_rows.append(
                    (
                        id_nota,
                        id_usuario,
                        tipo_evento,
                        random_date(250),
                        Json(metadata),
                    )
                )

            execute_values(
                cur,
                """
                INSERT INTO nota_evento (
                    id_nota,
                    id_usuario,
                    tipo_evento,
                    fecha_evento,
                    metadata
                )
                VALUES %s;
                """,
                evento_rows,
            )

            conn.commit()

            print("\nSeed terminado correctamente.")
            print(f"Usuarios: {len(usuario_ids)}")
            print(f"Materias: {len(materia_ids)}")
            print(f"Temas: {len(tema_ids)}")
            print(f"Notas: {len(nota_ids)}")
            print(f"Tags: {len(tag_ids)}")
            print(f"Fuentes: {len(fuente_ids)}")
            print(f"Nota-Tag: {len(nota_tag_pairs)}")
            print(f"Nota-Fuente: {len(nota_fuente_pairs)}")
            print(f"Links entre notas: {len(link_ids)}")
            print(f"Versiones: {len(version_rows)}")
            print(f"Eventos: {len(evento_rows)}")

    except Exception as e:
        conn.rollback()
        print("\nError durante el seed. Se hizo rollback.")
        raise e

    finally:
        conn.close()


if __name__ == "__main__":
    main()
