import { sql } from "bun";

export async function computeSimilarities(): Promise<void> {
  const targets = await sql`
    SELECT id FROM targets WHERE matched_source_id IS NULL ORDER BY id
  `;

  console.log(`[MATCH] Total: ${targets.length}`);

  const startTime = Date.now();

  for (let i = 0; i < targets.length; i++) {
    const { id } = targets[i];

    await sql`
      UPDATE targets t
      SET matched_source_id = s.id, similarity = 1 - (t.embedding <=> s.embedding)
      FROM sources s
      WHERE t.id = ${id}
        AND s.namespace = t.namespace
        AND s.id = (
          SELECT id FROM sources
          WHERE namespace = t.namespace
          ORDER BY embedding <=> t.embedding
          LIMIT 1
        )
    `;

    if ((i + 1) % 100 === 0 || i + 1 === targets.length) {
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = (i + 1) / elapsed;
      console.log(`[MATCH] ${i + 1}/${targets.length} - ${rate.toFixed(1)}/s`);
    }
  }

  console.log(`[MATCH] Done!`);
}
