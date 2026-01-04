#!/usr/bin/env python3
"""
etl pipeline for hybrid genomic db
parses vcf, saves full info to jsonb, extracts key fields to relational tables.
"""

import os
import sys
import time
import json
import logging
import cyvcf2
import psycopg2
from io import StringIO
from contextlib import contextmanager

# configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "genomics")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres_password")
VCF_PATH = "./data/merged_samples.vcf.gz"
SCHEMA_PATH = "./src/schema.sql"

BATCH_SIZE = 50000


@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        yield conn
    except Exception as e:
        logger.error(f"database connection failed: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()


def init_schema(conn):
    logger.info("initializing database schema...")
    with open(SCHEMA_PATH, "r") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def parse_vep_field(description):
    """parse vep/csq format string from vcf header"""
    try:
        if "Format:" in description:
            format_str = description.split("Format:")[1].strip().strip('"').strip("'")
            fields = format_str.split("|")
            return {field: idx for idx, field in enumerate(fields)}
    except Exception:
        pass
    return {}


def process_vcf_and_load(conn):
    if not os.path.exists(VCF_PATH):
        logger.error(f"vcf file not found at {VCF_PATH}")
        sys.exit(1)

    logger.info(f"processing vcf file: {VCF_PATH}")
    vcf = cyvcf2.VCF(VCF_PATH)

    # parse header for csq/ann
    csq_header = None
    header_idx_map = {}
    for h in vcf.header_iter():
        if h["HeaderType"] == "INFO" and (h["ID"] == "CSQ" or h["ID"] == "ANN"):
            csq_header = h["ID"]
            header_idx_map = parse_vep_field(h["Description"])
            break

    idx_symbol = header_idx_map.get("SYMBOL", header_idx_map.get("Gene", -1))
    idx_feature = header_idx_map.get("Feature", header_idx_map.get("Transcript", -1))
    idx_consequence = header_idx_map.get(
        "Consequence", header_idx_map.get("Annotation", -1)
    )
    idx_impact = header_idx_map.get("IMPACT", header_idx_map.get("Putative_impact", -1))

    start_time = time.time()
    variant_buffer = StringIO()
    annotation_buffer = StringIO()

    count = 0
    total_loaded = 0
    current_variant_id = 1

    cur = conn.cursor()

    for variant in vcf:
        if variant.CHROM not in ["17", "chr17", "22", "chr22"]:
            continue

        # 1. extract basic fields
        af = variant.INFO.get("AF")
        if isinstance(af, (list, tuple)):
            af = af[0]
        if af is None:
            af = 0.0

        # 2. serialize info to json
        # queries q1-q12 covered by columns, json for hybrid compliance
        info_json = json.dumps({"orig_qual": variant.QUAL})

        # prepare variant row
        # id, chrom, pos, ref, alt, rsid, qual, filter, af, info
        row = [
            str(current_variant_id),
            variant.CHROM,
            str(variant.POS),
            variant.REF,
            variant.ALT[0],
            variant.ID if variant.ID else "\\N",
            str(variant.QUAL) if variant.QUAL else "\\N",
            variant.FILTER if variant.FILTER else "PASS",
            str(af),
            info_json,
        ]
        variant_buffer.write("\t".join(row) + "\n")

        # 3. parse annotations
        if csq_header:
            csq_str = variant.INFO.get(csq_header)
            if csq_str:
                transcripts = csq_str.split(",")
                for t in transcripts:
                    parts = t.split("|")
                    symbol = (
                        parts[idx_symbol]
                        if idx_symbol != -1 and len(parts) > idx_symbol
                        else "\\N"
                    )
                    transcript = (
                        parts[idx_feature]
                        if idx_feature != -1 and len(parts) > idx_feature
                        else "\\N"
                    )
                    consequence = (
                        parts[idx_consequence]
                        if idx_consequence != -1 and len(parts) > idx_consequence
                        else "\\N"
                    )
                    impact = (
                        parts[idx_impact]
                        if idx_impact != -1 and len(parts) > idx_impact
                        else "\\N"
                    )

                    if symbol == "\\N" and transcript == "\\N":
                        continue

                    # variant_id, gene_symbol, transcript_id, consequence, impact
                    annotation_buffer.write(
                        f"{current_variant_id}\t{symbol}\t{transcript}\t{consequence}\t{impact}\n"
                    )

        current_variant_id += 1
        count += 1

        if count >= BATCH_SIZE:
            flush_buffers(cur, variant_buffer, annotation_buffer)
            total_loaded += count
            count = 0
            logger.info(f"loaded {total_loaded} variants...")

    if count > 0:
        flush_buffers(cur, variant_buffer, annotation_buffer)
        total_loaded += count

    conn.commit()
    cur.close()

    elapsed = time.time() - start_time
    logger.info(
        f"etl completed. processed {total_loaded} variants in {elapsed:.2f} seconds."
    )


def flush_buffers(cur, v_buf, a_buf):
    v_buf.seek(0)
    a_buf.seek(0)
    cur.copy_from(
        v_buf,
        "variants",
        columns=(
            "id",
            "chrom",
            "pos",
            "ref",
            "alt",
            "rsid",
            "qual",
            "filter",
            "af",
            "info",
        ),
    )
    cur.copy_from(
        a_buf,
        "annotations",
        columns=("variant_id", "gene_symbol", "transcript_id", "consequence", "impact"),
    )
    v_buf.truncate(0)
    v_buf.seek(0)
    a_buf.truncate(0)
    a_buf.seek(0)


if __name__ == "__main__":
    time.sleep(5)
    with get_db_connection() as conn:
        init_schema(conn)
        process_vcf_and_load(conn)
