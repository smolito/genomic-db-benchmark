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
    """
    Parse vep/csq format string from vcf header.
    Handles standard VEP 'Format: ...' and SnpEff 'Functional annotations: ...'
    """
    format_str = None
    try:
        # VEP style
        if "Format:" in description:
            format_str = description.split("Format:")[1]
        # SnpEff style
        elif "Functional annotations:" in description:
            format_str = description.split("Functional annotations:")[1]

        if format_str:
            # Clean up quotes and whitespace
            format_str = format_str.strip().strip('"').strip("'").strip()
            # Split by pipe and strip whitespace from each field
            fields = [f.strip() for f in format_str.split("|")]
            return {field: idx for idx, field in enumerate(fields)}

    except Exception as e:
        logger.warning(
            f"Failed to parse header description: {description} - Error: {e}"
        )
    return {}


def process_vcf_and_load(conn):
    if not os.path.exists(VCF_PATH):
        logger.error(f"vcf file not found at {VCF_PATH}")
        sys.exit(1)

    logger.info(f"processing vcf file: {VCF_PATH}")
    vcf = cyvcf2.VCF(VCF_PATH)

    # 1. Parse header for CSQ/ANN (relational part)
    csq_header = None
    header_idx_map = {}

    # 2. Collect all valid INFO keys from header
    info_keys_from_header = []

    for h in vcf.header_iter():
        if h["HeaderType"] == "INFO":
            # Store standard INFO keys
            info_keys_from_header.append(h["ID"])

            # Check for Annotation header
            if h["ID"] == "CSQ" or h["ID"] == "ANN":
                csq_header = h["ID"]
                header_idx_map = parse_vep_field(h["Description"])
                logger.info(
                    f"Found Annotation Header '{csq_header}' with fields: {list(header_idx_map.keys())[:5]}..."
                )

    if not csq_header:
        logger.warning("No CSQ or ANN header found! Annotations table will be empty.")

    # Try to map common field names to what we need
    keys = header_idx_map.keys()

    idx_symbol = -1
    for k in ["SYMBOL", "Gene", "gene_name", "GeneName"]:
        if k in header_idx_map:
            idx_symbol = header_idx_map[k]
            break

    idx_feature = -1
    for k in ["Feature", "Transcript", "transcript_id", "Feature_ID"]:
        if k in header_idx_map:
            idx_feature = header_idx_map[k]
            break

    idx_consequence = -1
    for k in ["Consequence", "Annotation", "effect"]:
        if k in header_idx_map:
            idx_consequence = header_idx_map[k]
            break

    idx_impact = -1
    for k in ["IMPACT", "Putative_impact", "impact"]:
        if k in header_idx_map:
            idx_impact = header_idx_map[k]
            break

    logger.info(
        f"Mapped indices - Symbol: {idx_symbol}, Transcript: {idx_feature}, Consequence: {idx_consequence}, Impact: {idx_impact}"
    )

    if idx_feature == -1 and csq_header:
        logger.warning(
            "WARNING: Transcript ID column could not be found in annotation header. Q9 will likely fail!"
        )

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

        # --- Base Fields ---
        af = variant.INFO.get("AF")
        if isinstance(af, (list, tuple)):
            af = af[0]
        if af is None:
            af = 0.0

        # Capture QUAL
        qual_val = variant.QUAL
        if qual_val is None:
            qual_val_str = "\\N"
            qual_json = None
        else:
            qual_val_str = str(qual_val)
            qual_json = qual_val

        # --- JSON Construction ---
        info_dict = {"orig_qual": qual_json}

        # Populate JSON with typed values
        for key in info_keys_from_header:
            val = variant.INFO.get(key)
            if val is not None:
                if isinstance(val, tuple):
                    val = list(val)
                info_dict[key] = val

        info_json = json.dumps(info_dict)

        # --- ID / RSID Handling (Fix for Q3) ---
        # Try standard ID column first
        rsid_val = variant.ID

        # Fallback: check INFO field if ID is missing or dot
        if not rsid_val or rsid_val == ".":
            rsid_val = variant.INFO.get("RS")
            # If it's a list (multiple RS IDs), take the first one or join them
            if isinstance(rsid_val, (list, tuple)):
                rsid_val = str(rsid_val[0])
            elif rsid_val is not None:
                rsid_val = str(rsid_val)

        # Final safety check for database string
        if not rsid_val or rsid_val == ".":
            rsid_val = "\\N"

        # prepare variant row
        # id, chrom, pos, ref, alt, rsid, qual, filter, af, info
        row = [
            str(current_variant_id),
            variant.CHROM,
            str(variant.POS),
            variant.REF,
            variant.ALT[0],
            rsid_val,
            qual_val_str,
            variant.FILTER if variant.FILTER else "PASS",
            str(af),
            info_json,
        ]
        variant_buffer.write("\t".join(row) + "\n")

        # --- Annotations (Relational) ---
        if csq_header:
            csq_val = variant.INFO.get(csq_header)
            if csq_val:
                if isinstance(csq_val, tuple):
                    csq_val = ",".join(csq_val)

                transcripts = csq_val.split(",")
                for t in transcripts:
                    parts = t.split("|")

                    symbol = (
                        parts[idx_symbol].strip()
                        if idx_symbol != -1
                        and len(parts) > idx_symbol
                        and parts[idx_symbol]
                        else "\\N"
                    )
                    transcript = (
                        parts[idx_feature].strip()
                        if idx_feature != -1
                        and len(parts) > idx_feature
                        and parts[idx_feature]
                        else "\\N"
                    )
                    consequence = (
                        parts[idx_consequence].strip()
                        if idx_consequence != -1
                        and len(parts) > idx_consequence
                        and parts[idx_consequence]
                        else "\\N"
                    )
                    impact = (
                        parts[idx_impact].strip()
                        if idx_impact != -1
                        and len(parts) > idx_impact
                        and parts[idx_impact]
                        else "\\N"
                    )

                    # Skip empty annotations
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
