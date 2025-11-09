import { sql } from "bun";

export async function initDatabase() {
  console.log("Initializing database...");

  await sql`CREATE EXTENSION IF NOT EXISTS vector`;
  console.log("[OK] Extension enabled");

  await sql`
    CREATE TABLE IF NOT EXISTS sources (
      id BIGSERIAL PRIMARY KEY,
      data JSONB NOT NULL,
      embedding vector(1024) NOT NULL
    )
  `;
  console.log("[OK] sources table ready");

  await sql`
    CREATE INDEX IF NOT EXISTS idx_sources_embedding ON sources 
    USING hnsw (embedding vector_cosine_ops)
  `;
  console.log("[OK] sources index ready");

  await sql`
    CREATE TABLE IF NOT EXISTS targets (
      id BIGSERIAL PRIMARY KEY,
      data JSONB NOT NULL,
      embedding vector(1024) NOT NULL,
      matched_source_id BIGINT REFERENCES sources(id) ON DELETE SET NULL,
      similarity DOUBLE PRECISION CHECK (similarity >= 0 AND similarity <= 1)
    )
  `;
  console.log("[OK] targets table ready");

  await sql`
    CREATE INDEX IF NOT EXISTS idx_targets_embedding ON targets 
    USING hnsw (embedding vector_cosine_ops)
  `;
  console.log("[OK] targets index ready");

  await sql`
    CREATE INDEX IF NOT EXISTS idx_targets_matched_source_id ON targets(matched_source_id)
  `;
  console.log("[OK] foreign key index ready");

  console.log("[SUCCESS] Database initialized successfully!");
}
