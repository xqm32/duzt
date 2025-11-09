import { initDatabase } from "./db";
import { loadSources, loadTargets } from "./load";

await initDatabase();
await loadSources();
await loadTargets();
