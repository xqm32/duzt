import { embedMany } from "ai";
import { loadCSV, table } from "arquero";
import { Glob, sql } from "bun";
import { model } from "./ai";

const SKIP_ROWS = Number(process.env.SKIP_ROWS) || 0;
const BATCH_SIZE = Number(process.env.CHUNK_SIZE) || 1000;
const NAMESPACE_COLUMN = process.env.NAMESPACE_COLUMN!;
const SOURCE_COLUMN = process.env.SOURCE_COLUMN!;
const TARGET_COLUMN = process.env.TARGET_COLUMN!;

type TableType = "sources" | "targets";
type CsvRow = Record<string, string>;

const embeddingCache = new Map<string, number[]>();

interface FailedBatch {
  fileName: string;
  batchNumber: number;
  rows: CsvRow[];
  error: string;
  timestamp: string;
}

function getValueColumn(tableType: TableType): string {
  return tableType === "sources" ? SOURCE_COLUMN : TARGET_COLUMN;
}

function isValidRow(row: CsvRow, valueColumn: string): boolean {
  const value = row[valueColumn];
  return value != null && String(value).trim() !== "";
}

async function loadCsvFile(filePath: string): Promise<CsvRow[]> {
  const csvTable = await loadCSV(filePath, { skip: SKIP_ROWS });
  return csvTable.objects() as CsvRow[];
}

function filterValidRows(rows: CsvRow[], valueColumn: string): CsvRow[] {
  return rows.filter((row) => isValidRow(row, valueColumn));
}

async function insertRowsWithEmbeddings(
  tableType: TableType,
  rows: CsvRow[],
  embeddings: number[][],
  valueColumn: string
): Promise<void> {
  if (rows.length !== embeddings.length) {
    throw new Error("Rows and embeddings length mismatch");
  }

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const embedding = embeddings[i];

    if (!row || !embedding) {
      continue;
    }

    const value = String(row[valueColumn]);
    const namespace = row[NAMESPACE_COLUMN];

    await sql`
      INSERT INTO ${sql(tableType)} (namespace, data, value, embedding)
      VALUES (${namespace}, ${row}, ${value}, ${sql.array(embedding, "REAL")})
    `;
  }
}

async function getCachedEmbedding(
  tableType: TableType,
  value: string
): Promise<number[] | null> {
  if (embeddingCache.has(value)) {
    return embeddingCache.get(value)!;
  }

  const [row] = await sql`
    SELECT embedding::text FROM ${sql(tableType)} 
    WHERE value = ${value} 
    LIMIT 1
  `;

  if (row) {
    const embedding = JSON.parse(row.embedding);
    embeddingCache.set(value, embedding);
    return embedding;
  }

  return null;
}

async function processBatch(
  tableType: TableType,
  batchRows: CsvRow[],
  valueColumn: string,
  batchNumber: number,
  totalBatches: number
): Promise<FailedBatch | null> {
  const logPrefix = tableType.toUpperCase();
  console.log(`[${logPrefix}] Batch ${batchNumber}/${totalBatches}`);

  try {
    const values = batchRows.map((row) => String(row[valueColumn]));
    const embeddings: (number[] | null)[] = await Promise.all(
      values.map((value) => getCachedEmbedding(tableType, value))
    );

    const valuesToEmbed: string[] = [];
    const indicesToFill: number[] = [];

    for (let i = 0; i < embeddings.length; i++) {
      if (!embeddings[i]) {
        valuesToEmbed.push(values[i]!);
        indicesToFill.push(i);
      }
    }

    if (valuesToEmbed.length > 0) {
      const { embeddings: newEmbeddings } = await embedMany({
        model,
        values: valuesToEmbed,
      });

      for (let i = 0; i < valuesToEmbed.length; i++) {
        const index = indicesToFill[i]!;
        const embedding = newEmbeddings[i]!;
        embeddings[index] = embedding;
        embeddingCache.set(valuesToEmbed[i]!, embedding);
      }

      const cacheHits = values.length - valuesToEmbed.length;
      console.log(
        `[${logPrefix}] Cached: ${cacheHits}/${values.length}, New: ${valuesToEmbed.length}`
      );
    } else {
      console.log(`[${logPrefix}] All cached`);
    }

    await insertRowsWithEmbeddings(
      tableType,
      batchRows,
      embeddings as number[][],
      valueColumn
    );
    return null;
  } catch (error) {
    console.error(`[${logPrefix}] Batch ${batchNumber} failed:`, error);
    return {
      fileName: "",
      batchNumber,
      rows: batchRows,
      error: String(error),
      timestamp: new Date().toISOString(),
    };
  }
}

async function saveFailedBatchesToFiles(
  tableType: TableType,
  fileName: string,
  failedBatches: FailedBatch[]
): Promise<void> {
  if (failedBatches.length === 0) {
    return;
  }

  const timestamp = Date.now();
  const logPrefix = tableType.toUpperCase();

  const allFailedRows = failedBatches.flatMap((batch) => batch.rows);

  const failedCsvFileName = `failed_${tableType}_${fileName}_${timestamp}.csv`;
  const csvTable = table(allFailedRows);
  const csvContent = csvTable.toCSV();
  await Bun.write(failedCsvFileName, csvContent);

  const errorLogFileName = `failed_${tableType}_${fileName}_${timestamp}.json`;
  const errorLog = failedBatches.map((batch) => ({
    fileName,
    batchNumber: batch.batchNumber,
    rowCount: batch.rows.length,
    error: batch.error,
    timestamp: batch.timestamp,
  }));
  await Bun.write(errorLogFileName, JSON.stringify(errorLog, null, 2));

  const totalFailedRows = allFailedRows.length;
  const batchCount = failedBatches.length;
  console.error(
    `[${logPrefix}] ${batchCount} batches (${totalFailedRows} rows) failed`
  );
  console.error(`[${logPrefix}] Failed rows saved to ${failedCsvFileName}`);
  console.error(`[${logPrefix}] Error log saved to ${errorLogFileName}`);
}

async function loadFileData(
  filePath: string,
  tableType: TableType
): Promise<void> {
  const fileName = filePath.split("/").pop()!;
  const logPrefix = tableType.toUpperCase();
  const valueColumn = getValueColumn(tableType);

  console.log(`[${logPrefix}] Loading ${fileName}`);

  const allRows = await loadCsvFile(filePath);
  const validRows = filterValidRows(allRows, valueColumn);

  if (validRows.length === 0) {
    console.log(`[${logPrefix}] No valid data in ${fileName}`);
    return;
  }

  console.log(`[${logPrefix}] Processing ${validRows.length} rows`);

  const failedBatches: FailedBatch[] = [];
  const totalBatches = Math.ceil(validRows.length / BATCH_SIZE);

  for (let i = 0; i < validRows.length; i += BATCH_SIZE) {
    const batchRows = validRows.slice(i, i + BATCH_SIZE);
    const batchNumber = Math.floor(i / BATCH_SIZE) + 1;

    const failedBatch = await processBatch(
      tableType,
      batchRows,
      valueColumn,
      batchNumber,
      totalBatches
    );

    if (failedBatch) {
      failedBatches.push(failedBatch);
    }
  }

  await saveFailedBatchesToFiles(tableType, fileName, failedBatches);
}

async function loadTableData(tableType: TableType): Promise<void> {
  const pattern = `${tableType}*.csv`;
  const csvFiles = Array.from(new Glob(pattern).scanSync("."));

  if (csvFiles.length === 0) {
    return;
  }

  const logPrefix = tableType.toUpperCase();
  console.log(`[${logPrefix}] Found ${csvFiles.length} files`);

  for (const file of csvFiles) {
    await loadFileData(file, tableType);
  }
}

export const loadSources = () => loadTableData("sources");
export const loadTargets = () => loadTableData("targets");
