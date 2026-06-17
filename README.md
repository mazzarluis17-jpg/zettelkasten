# Zettelkasten Multi-DBMS

A Polyglot Persistence Knowledge Management System

## Overview

This project implements a Zettelkasten-inspired knowledge management system using multiple database paradigms.

Instead of relying on a single database engine, the system uses a polyglot persistence architecture where each database is optimized for a specific workload.

## Architecture

The system consists of:

* PostgreSQL (System of Record)
* MongoDB (Document Store)
* Neo4j (Graph Database)
* ClickHouse (Analytical Database)
* Metabase (Visualization Layer)

## Motivation

A Zettelkasten naturally combines:

* Structured metadata
* Rich document content
* Complex graph relationships
* Analytical workloads

No single database excels at all these tasks simultaneously.

## Technologies

* Python
* Docker Compose
* PostgreSQL
* MongoDB
* Neo4j
* ClickHouse
* Metabase

## ETL Pipeline

1. Seed PostgreSQL with reproducible data.
2. Export relational data to MongoDB.
3. Generate graph structures in Neo4j.
4. Create analytical tables in ClickHouse.
5. Execute benchmark queries.

## Benchmarks

The project compares database performance across:

* OLAP aggregations
* Rich document retrieval
* Graph traversals
* Analytical rankings

## Key Findings

* PostgreSQL provides transactional consistency.
* MongoDB excels at document retrieval.
* Neo4j performs best for graph traversal.
* ClickHouse dominates analytical workloads.

## Author

Luis Alfredo Ramírez Maza

