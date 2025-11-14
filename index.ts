import { initDatabase } from "./db";
import { loadSources, loadTargets } from "./load";
import { computeSimilarities } from "./match";

await initDatabase();
await loadSources();
await loadTargets();
await computeSimilarities();
