-- hybrid schema: relational core + jsonb

DROP TABLE IF EXISTS annotations;
DROP TABLE IF EXISTS variants;

-- 1. variants table (the "hybrid" core)
CREATE TABLE variants (
    id BIGSERIAL PRIMARY KEY,
    chrom TEXT NOT NULL,
    pos INTEGER NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,
    rsid TEXT,         -- extracted for q3
    qual REAL,
    filter TEXT,
    af REAL DEFAULT 0.0, -- extracted for q12 (allele frequency)
    info JSONB         -- full info field for flexibility (the "hybrid" part)
);

-- 2. annotations table (normalized for search)
-- solves the 1:n problem (one variant -> multiple genes/transcripts)
-- essential for q4, q5, q9, q10
CREATE TABLE annotations (
    id BIGSERIAL PRIMARY KEY,
    variant_id BIGINT NOT NULL, -- logical link (foreign key omitted for load speed)
    gene_symbol TEXT,      -- for q4, q5, q11, q12
    transcript_id TEXT,    -- for q9
    consequence TEXT,      -- for q10 (e.g., missense_variant)
    impact TEXT            -- high, moderate, low, modifier
);

-- --- indexes (created after load for performance, defined here for reference) ---

-- position-based lookups (q1, q2, q6, q7, q8)
CREATE INDEX idx_variants_chrom_pos ON variants(chrom, pos);

-- single key lookups
CREATE INDEX idx_variants_rsid ON variants(rsid) WHERE rsid IS NOT NULL; -- q3
CREATE INDEX idx_variants_af ON variants(af) WHERE af < 0.05;            -- q12 (sparse index for rare variants)

-- gene & transcript lookups (the heavy lifters for q4-q11)
CREATE INDEX idx_annotations_gene ON annotations(gene_symbol);
CREATE INDEX idx_annotations_transcript ON annotations(transcript_id);
CREATE INDEX idx_annotations_consequence ON annotations(consequence);

-- join optimization
CREATE INDEX idx_annotations_variant_id ON annotations(variant_id);