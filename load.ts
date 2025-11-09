import { embedMany } from "ai";
import { loadCSV } from "arquero";
import { sql } from "bun";
import { chunk, zip } from "lodash";
import { model } from "./ai";

const SKIP_ROWS = Number(process.env.SKIP_ROWS) || 0;
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE) || 1000;
const NAMESPACE_COLUMN = process.env.NAMESPACE_COLUMN!;
const SOURCE_COLUMN = process.env.SOURCE_COLUMN!;
const TARGET_COLUMN = process.env.TARGET_COLUMN!;

async function loadData(type: "sources" | "targets") {
  const typeLabel = type.toUpperCase();
  const filePath = `${type}.csv`;

  if (!(await Bun.file(filePath).exists())) {
    console.log(`[${typeLabel}] File ${filePath} does not exist, skipping`);
    return;
  }

  console.log(`[${typeLabel}] Start loading from ${filePath}`);

  const csvData = await loadCSV(filePath, { skip: SKIP_ROWS });
  const totalRows = csvData.numRows();
  console.log(`[${typeLabel}] Found ${totalRows} rows`);

  const batches = chunk(csvData.objects(), CHUNK_SIZE);
  const totalBatches = batches.length;
  console.log(
    `[${typeLabel}] Processing ${totalBatches} batches (${CHUNK_SIZE} rows per batch)`
  );

  for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
    const currentBatch = batchIndex + 1;
    const progress = ((currentBatch / totalBatches) * 100).toFixed(2);
    console.log(
      `[${typeLabel}] Batch ${currentBatch}/${totalBatches} (${progress}%)`
    );

    const rows = batches[batchIndex] as Record<string, string>[];
    const columnMap = { sources: SOURCE_COLUMN, targets: TARGET_COLUMN };
    const values = rows.map((row) => row[columnMap[type]]);
    const { embeddings } = await embedMany({ model, values });

    for (const [rowData, embedding] of zip(rows, embeddings)) {
      const namespace = rowData![NAMESPACE_COLUMN];
      await sql`
        INSERT INTO ${sql(type)} (namespace, data, embedding)
        VALUES (${namespace}, ${rowData}, ${sql.array(embedding!, "REAL")})
      `;
    }
  }

  console.log(`[${typeLabel}] Successfully loaded all ${totalRows} rows`);
}

export const loadSources = async () => await loadData("sources");

export const loadTargets = async () => await loadData("targets");
