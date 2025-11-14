import { sql } from "bun";

const BATCH_SIZE = Number(process.env.MATCH_BATCH_SIZE) || 10000;

async function processBatch(limit: number) {
  const startTime = Date.now();

  const result = await sql`
    UPDATE targets t
    SET
      matched_source_id = s.id,
      similarity = 1 - (t.embedding <=> s.embedding)
    FROM (
      SELECT id, namespace, embedding
      FROM targets
      WHERE matched_source_id IS NULL
      ORDER BY id
      LIMIT ${limit}
    ) b
    CROSS JOIN LATERAL (
      SELECT id, embedding
      FROM sources s
      WHERE s.namespace = b.namespace
      ORDER BY b.embedding <=> s.embedding
      LIMIT 1
    ) s
    WHERE t.id = b.id
  `;

  const elapsed = (Date.now() - startTime) / 1000;
  const rate = elapsed > 0 ? result.count / elapsed : 0;

  return { count: result.count, elapsed, rate };
}

export async function computeSimilarities(): Promise<void> {
  console.log("[MATCH] Starting similarity computation...");

  await sql`SET work_mem = '1GB'`;
  await sql`SET maintenance_work_mem = '2GB'`;
  await sql`SET effective_cache_size = '12GB'`;
  await sql`SET max_parallel_workers_per_gather = 4`;
  await sql`SET random_page_cost = 1.1`;

  const [{ count: totalCount }] = await sql`
    SELECT COUNT(*) as count FROM targets WHERE matched_source_id IS NULL
  `;

  if (totalCount === 0) {
    console.log("[MATCH] No targets to process");
    return;
  }

  console.log(`[MATCH] Total: ${totalCount}, Batch: ${BATCH_SIZE}`);

  let processed = 0;

  while (processed < totalCount) {
    const { count, elapsed, rate } = await processBatch(BATCH_SIZE);

    if (count === 0) break;

    processed += count;
    const progress = ((processed / totalCount) * 100).toFixed(1);
    console.log(
      `[MATCH] ${processed}/${totalCount} (${progress}%) - ${elapsed.toFixed(
        2
      )}s, ${rate.toFixed(1)}/s`
    );
  }

  console.log(`[MATCH] Completed! Processed: ${processed}`);
}
