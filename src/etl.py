#!/usr/bin/env python3
"""
etl pipeline for hybrid genomic db
parses vcf, saves full info to jsonb, extracts key fields to relational tables.
fixes: rsid extraction from csq, transcript id mapping, header priority.
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
DB_PORT = os.getenv("DB_PORT", "5478")
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
    parse vep/csq format string from vcf header.
    """
    format_str = None
    try:
        # vep style
        if "Format:" in description:
            format_str = description.split("Format:")[1]
        # snpeff style
        elif "Functional annotations:" in description:
            format_str = description.split("Functional annotations:")[1]

        if format_str:
            format_str = format_str.strip().strip('"').strip("'").strip()
            fields = [f.strip() for f in format_str.split("|")]
            return {field: idx for idx, field in enumerate(fields)}

    except Exception as e:
        logger.warning(
            f"failed to parse header description: {description} - error: {e}"
        )
    return {}


def process_vcf_and_load(conn):
    if not os.path.exists(VCF_PATH):
        logger.error(f"vcf file not found at {VCF_PATH}")
        sys.exit(1)

    logger.info(f"processing vcf file: {VCF_PATH}")
    vcf = cyvcf2.VCF(VCF_PATH)

    # 1. header parsing strategy: prioritize csq (vep) over ann (snpeff)
    csq_header = None
    header_idx_map = {}
    info_keys_from_header = []

    # temporary storage to handle priority
    found_headers = {}

    for h in vcf.header_iter():
        if h["HeaderType"] == "INFO":
            info_keys_from_header.append(h["ID"])
            if h["ID"] in ["CSQ", "ANN"]:
                found_headers[h["ID"]] = h["Description"]

    # priority logic: use csq if available, else ann
    if "CSQ" in found_headers:
        csq_header = "CSQ"
        header_idx_map = parse_vep_field(found_headers["CSQ"])
    elif "ANN" in found_headers:
        csq_header = "ANN"
        header_idx_map = parse_vep_field(found_headers["ANN"])

    if not csq_header:
        logger.warning("no csq or ann header found! annotations table will be empty.")
    else:
        logger.info(f"selected annotation field: {csq_header}")

    # map indices based on the selected header
    # symbol mapping
    idx_symbol = -1
    for k in ["SYMBOL", "Gene", "gene_name", "GeneName"]:
        if k in header_idx_map:
            idx_symbol = header_idx_map[k]
            break

    # feature/transcript id mapping
    # note: 'feature' is standard vep for transcript id (enst...), 'feature_id' for snpeff
    idx_feature = -1
    for k in ["Feature", "Feature_ID", "Transcript", "transcript_id"]:
        if k in header_idx_map:
            idx_feature = header_idx_map[k]
            break

    # consequence mapping
    idx_consequence = -1
    for k in ["Consequence", "Annotation", "effect"]:
        if k in header_idx_map:
            idx_consequence = header_idx_map[k]
            break

    # impact mapping
    idx_impact = -1
    for k in ["IMPACT", "Putative_impact", "impact"]:
        if k in header_idx_map:
            idx_impact = header_idx_map[k]
            break

    # rsid/existing variation mapping (specific to csq)
    idx_existing_var = -1
    if csq_header == "CSQ":
        for k in ["Existing_variation", "ID", "id"]:
            if k in header_idx_map:
                idx_existing_var = header_idx_map[k]
                break

    logger.info(
        f"mapped indices -> symbol: {idx_symbol}, transcript: {idx_feature}, "
        f"consequence: {idx_consequence}, impact: {idx_impact}, existing_var: {idx_existing_var}"
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

        # --- base fields ---
        af = variant.INFO.get("AF")
        if isinstance(af, (list, tuple)):
            af = af[0]
        if af is None:
            af = 0.0

        qual_val = variant.QUAL
        if qual_val is None:
            qual_val_str = "\\N"
            qual_json = None
        else:
            qual_val_str = str(qual_val)
            qual_json = qual_val

        # --- json construction ---
        info_dict = {"orig_qual": qual_json}
        for key in info_keys_from_header:
            val = variant.INFO.get(key)
            if val is not None:
                if isinstance(val, tuple):
                    val = list(val)
                info_dict[key] = val
        info_json = json.dumps(info_dict)

        # --- annotation parsing & rsid extraction ---
        csq_val = variant.INFO.get(csq_header)
        csq_rsid = None

        # parse annotations to buffer (and find rsid if possible)
        if csq_val:
            if isinstance(csq_val, tuple):
                csq_val = ",".join(csq_val)

            transcripts = csq_val.split(",")

            # check the first transcript for rsid if we have the index
            if idx_existing_var != -1 and len(transcripts) > 0:
                first_parts = transcripts[0].split("|")
                if len(first_parts) > idx_existing_var:
                    raw_vars = first_parts[idx_existing_var]
                    # existing_variation can be "rs123&COSV456"
                    if raw_vars:
                        for v in raw_vars.split("&"):
                            if v.startswith("rs"):
                                csq_rsid = v
                                break

            for t in transcripts:
                parts = t.split("|")

                symbol = (
                    parts[idx_symbol].strip()
                    if idx_symbol != -1
                    and len(parts) > idx_symbol
                    and parts[idx_symbol]
                    else "\\N"
                )

                # clean transcript id (remove version numbers if desired, e.g., .10)
                # keeping it raw for now based on schema
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

                if symbol == "\\N" and transcript == "\\N":
                    continue

                annotation_buffer.write(
                    f"{current_variant_id}\t{symbol}\t{transcript}\t{consequence}\t{impact}\n"
                )

        # --- id / rsid resolution ---
        # 1. try vcf id column
        rsid_val = variant.ID

        # 2. fallback to info['rs'] tag (rarely used if csq is present)
        if not rsid_val or rsid_val == ".":
            rsid_val = variant.INFO.get("RS")
            if isinstance(rsid_val, (list, tuple)):
                rsid_val = str(rsid_val[0])
            elif rsid_val is not None:
                rsid_val = str(rsid_val)

        # 3. fallback to extracted csq rsid
        if (not rsid_val or rsid_val == ".") and csq_rsid:
            rsid_val = csq_rsid

        if not rsid_val or rsid_val == ".":
            rsid_val = "\\N"

        # --- write variant ---
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
