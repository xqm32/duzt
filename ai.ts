import { createOpenAICompatible } from "@ai-sdk/openai-compatible";

export const provider = createOpenAICompatible({
  name: "provider",
  baseURL: process.env.PROVIDER_BASE_URL!,
  apiKey: process.env.PROVIDER_API_KEY!,
});

export const model = provider.textEmbeddingModel(
  process.env.PROVIDER_EMBEDDING_MODEL!
);
