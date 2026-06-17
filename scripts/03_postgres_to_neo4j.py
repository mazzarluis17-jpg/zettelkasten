import os

import psycopg2
from psycopg2.extras import RealDictCursor
from neo4j import GraphDatabase


POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "zettelkasten"),
    "user": os.getenv("POSTGRES_USER", "zettel_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "zettel_pass"),
}

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "zettel_pass")


def fetch_all(cur, query):
    cur.execute(query)
    return cur.fetchall()


def run_write_batch(session, query, rows, batch_size=500):
    total = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        session.execute_write(lambda tx: tx.run(query, rows=batch).consume())
        total += len(batch)

    return total


def main():
    print("Conectando a PostgreSQL...")
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)

    print("Conectando a Neo4j...")
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )

    try:
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            print("Leyendo usuarios...")
            usuarios = fetch_all(cur, """
                SELECT id_usuario, nombre, email, rol
                FROM usuario
                ORDER BY id_usuario;
            """)

            print("Leyendo materias...")
            materias = fetch_all(cur, """
                SELECT id_materia, nombre, descripcion
                FROM materia
                ORDER BY id_materia;
            """)

            print("Leyendo temas...")
            temas = fetch_all(cur, """
                SELECT id_tema, id_materia, nombre, descripcion
                FROM tema
                ORDER BY id_tema;
            """)

            print("Leyendo notas...")
            notas = fetch_all(cur, """
                SELECT
                    id_nota,
                    id_usuario,
                    id_tema,
                    titulo,
                    slug,
                    tipo,
                    estado,
                    fecha_creacion,
                    fecha_actualizacion
                FROM nota
                ORDER BY id_nota;
            """)

            print("Leyendo tags...")
            tags = fetch_all(cur, """
                SELECT id_tag, nombre, descripcion
                FROM tag
                ORDER BY id_tag;
            """)

            print("Leyendo fuentes...")
            fuentes = fetch_all(cur, """
                SELECT id_fuente, tipo, titulo, autor, url, anio
                FROM fuente
                ORDER BY id_fuente;
            """)

            print("Leyendo nota_tag...")
            nota_tag = fetch_all(cur, """
                SELECT nt.id_nota, nt.id_tag
                FROM nota_tag nt;
            """)

            print("Leyendo nota_fuente...")
            nota_fuente = fetch_all(cur, """
                SELECT nf.id_nota, nf.id_fuente, nf.cita, nf.pagina
                FROM nota_fuente nf;
            """)

            print("Leyendo links entre notas...")
            nota_links = fetch_all(cur, """
                SELECT
                    id_link,
                    id_nota_origen,
                    id_nota_destino,
                    tipo_link,
                    contexto,
                    peso::float AS peso,
                    fecha_creacion::text AS fecha_creacion
                FROM nota_link
                ORDER BY id_link;
            """)

        with driver.session() as session:
            print("Limpiando Neo4j...")
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n").consume())

            print("Creando constraints...")
            constraints = [
                """
                CREATE CONSTRAINT usuario_email IF NOT EXISTS
                FOR (u:Usuario)
                REQUIRE u.email IS UNIQUE
                """,
                """
                CREATE CONSTRAINT materia_nombre IF NOT EXISTS
                FOR (m:Materia)
                REQUIRE m.nombre IS UNIQUE
                """,
                """
                CREATE CONSTRAINT tema_id IF NOT EXISTS
                FOR (t:Tema)
                REQUIRE t.id_tema IS UNIQUE
                """,
                """
                CREATE CONSTRAINT nota_slug IF NOT EXISTS
                FOR (n:Nota)
                REQUIRE n.slug IS UNIQUE
                """,
                """
                CREATE CONSTRAINT tag_nombre IF NOT EXISTS
                FOR (t:Tag)
                REQUIRE t.nombre IS UNIQUE
                """,
                """
                CREATE CONSTRAINT fuente_id IF NOT EXISTS
                FOR (f:Fuente)
                REQUIRE f.id_fuente IS UNIQUE
                """,
            ]

            for query in constraints:
                session.execute_write(lambda tx, q=query: tx.run(q).consume())

            print("Insertando nodos Usuario...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MERGE (u:Usuario {email: row.email})
                SET
                    u.nombre = row.nombre,
                    u.rol = row.rol
                """,
                usuarios,
            )

            print("Insertando nodos Materia...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MERGE (m:Materia {nombre: row.nombre})
                SET
                    m.id_materia = row.id_materia,
                    m.descripcion = row.descripcion
                """,
                materias,
            )

            print("Insertando nodos Tema...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MERGE (t:Tema {id_tema: row.id_tema})
                SET
                    t.nombre = row.nombre,
                    t.descripcion = row.descripcion
                """,
                temas,
            )

            print("Relacionando Tema -> Materia...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (t:Tema {id_tema: row.id_tema})
                MATCH (m:Materia {id_materia: row.id_materia})
                MERGE (t)-[:ES_PARTE_DE]->(m)
                """,
                temas,
            )

            print("Insertando nodos Nota...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MERGE (n:Nota {slug: row.slug})
                SET
                    n.id_nota = row.id_nota,
                    n.titulo = row.titulo,
                    n.tipo = row.tipo,
                    n.estado = row.estado,
                    n.fecha_creacion = toString(row.fecha_creacion),
                    n.fecha_actualizacion = toString(row.fecha_actualizacion)
                """,
                notas,
            )

            print("Relacionando Usuario -> Nota...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (n:Nota {id_nota: row.id_nota})
                MATCH (u:Usuario {email: row.email})
                MERGE (u)-[:CREO]->(n)
                """,
                [
                    {
                        "id_nota": n["id_nota"],
                        "email": next(
                            u["email"]
                            for u in usuarios
                            if u["id_usuario"] == n["id_usuario"]
                        )
                    }
                    for n in notas
                    if n["id_usuario"] is not None
                ],
            )

            print("Relacionando Nota -> Tema...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (n:Nota {id_nota: row.id_nota})
                MATCH (t:Tema {id_tema: row.id_tema})
                MERGE (n)-[:PERTENECE_A]->(t)
                """,
                [
                    {
                        "id_nota": n["id_nota"],
                        "id_tema": n["id_tema"]
                    }
                    for n in notas
                    if n["id_tema"] is not None
                ],
            )

            print("Insertando nodos Tag...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MERGE (t:Tag {nombre: row.nombre})
                SET
                    t.id_tag = row.id_tag,
                    t.descripcion = row.descripcion
                """,
                tags,
            )

            print("Relacionando Nota -> Tag...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (n:Nota {id_nota: row.id_nota})
                MATCH (t:Tag {id_tag: row.id_tag})
                MERGE (n)-[:TIENE_TAG]->(t)
                """,
                nota_tag,
            )

            print("Insertando nodos Fuente...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MERGE (f:Fuente {id_fuente: row.id_fuente})
                SET
                    f.tipo = row.tipo,
                    f.titulo = row.titulo,
                    f.autor = row.autor,
                    f.url = row.url,
                    f.anio = row.anio
                """,
                fuentes,
            )

            print("Relacionando Nota -> Fuente...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (n:Nota {id_nota: row.id_nota})
                MATCH (f:Fuente {id_fuente: row.id_fuente})
                MERGE (n)-[r:USA_FUENTE]->(f)
                SET
                    r.cita = row.cita,
                    r.pagina = row.pagina
                """,
                nota_fuente,
            )

            print("Insertando relaciones tipo Obsidian/Zettelkasten...")
            run_write_batch(
                session,
                """
                UNWIND $rows AS row
                MATCH (origen:Nota {id_nota: row.id_nota_origen})
                MATCH (destino:Nota {id_nota: row.id_nota_destino})
                MERGE (origen)-[r:LINK {
                    id_link: row.id_link
                }]->(destino)
                SET
                    r.tipo = row.tipo_link,
                    r.contexto = row.contexto,
                    r.peso = row.peso,
                    r.fecha_creacion = row.fecha_creacion
                """,
                nota_links,
            )

            print("\nCarga PostgreSQL → Neo4j terminada correctamente.")
            print(f"Usuarios: {len(usuarios)}")
            print(f"Materias: {len(materias)}")
            print(f"Temas: {len(temas)}")
            print(f"Notas: {len(notas)}")
            print(f"Tags: {len(tags)}")
            print(f"Fuentes: {len(fuentes)}")
            print(f"Nota-Tag: {len(nota_tag)}")
            print(f"Nota-Fuente: {len(nota_fuente)}")
            print(f"Links entre notas: {len(nota_links)}")

    finally:
        pg_conn.close()
        driver.close()


if __name__ == "__main__":
    main()
