import { sql } from "bun";

export async function initDatabase() {
  console.log("Initializing database...");

  await sql`CREATE EXTENSION IF NOT EXISTS vector`;
  console.log("[OK] Extension enabled");

  await sql`
    CREATE TABLE IF NOT EXISTS sources (
      id BIGSERIAL PRIMARY KEY,
      namespace TEXT NOT NULL DEFAULT 'default',
      data JSONB NOT NULL,
      value TEXT NOT NULL,
      embedding vector(1024) NOT NULL,
      UNIQUE(id, namespace)
    )
  `;
  console.log("[OK] sources table ready");

  await sql`
    CREATE INDEX IF NOT EXISTS idx_sources_embedding ON sources 
    USING hnsw (embedding vector_cosine_ops)
  `;
  console.log("[OK] sources index ready");

  await sql`
    CREATE INDEX IF NOT EXISTS idx_sources_namespace ON sources(namespace)
  `;
  console.log("[OK] sources namespace index ready");

  await sql`
    CREATE INDEX IF NOT EXISTS idx_sources_value ON sources(value)
  `;
  console.log("[OK] sources value index ready");

  await sql`
    CREATE TABLE IF NOT EXISTS targets (
      id BIGSERIAL PRIMARY KEY,
      namespace TEXT NOT NULL DEFAULT 'default',
      data JSONB NOT NULL,
      value TEXT NOT NULL,
      embedding vector(1024) NOT NULL,
      matched_source_id BIGINT,
      similarity DOUBLE PRECISION CHECK (similarity >= 0 AND similarity <= 1),
      FOREIGN KEY (matched_source_id, namespace) 
        REFERENCES sources(id, namespace) 
        ON DELETE SET NULL
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

  await sql`
    CREATE INDEX IF NOT EXISTS idx_targets_namespace ON targets(namespace)
  `;
  console.log("[OK] targets namespace index ready");

  await sql`
    CREATE INDEX IF NOT EXISTS idx_targets_value ON targets(value)
  `;
  console.log("[OK] targets value index ready");

  console.log("[SUCCESS] Database initialized successfully!");
}
