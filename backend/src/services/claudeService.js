import {
  BedrockRuntimeClient,
  InvokeModelCommand
} from "@aws-sdk/client-bedrock-runtime";

import { getJsonFromS3 } from "./s3Service.js";
import {
  analyzePdf,
  extractTextChunks,
  splitPdfIntoBatches,
  mergeExtractionResults,
  PDF_CONFIG
} from "./pdfChunkService.js";

const bedrockClient = new BedrockRuntimeClient({
  region: process.env.AWS_REGION
});

/* ------------------------------------------------ */
/* RETRY LOGIC (unchanged) */
/* ------------------------------------------------ */

const executeWithRetry = async (fn, retries = 3, delay = 2000) => {
  let attempt = 0;

  while (attempt < retries) {
    try {
      return await fn();
    } catch (error) {
      attempt++;

      if (attempt >= retries) {
        throw error;
      }

      console.warn(`Retrying... Attempt ${attempt}`);
      await new Promise((res) => setTimeout(res, delay * attempt));
    }
  }
};

/* ------------------------------------------------ */
/* CLAUDE CHUNK ANALYSIS */
/* ------------------------------------------------ */

async function analyzeChunk(chunkText, systemPrompt) {
  const userContent = chunkText;

  const response = await executeWithRetry(async () => {
  const command = new InvokeModelCommand({
    modelId: "anthropic.claude-3-sonnet-20240229-v1:0",
    contentType: "application/json",
    accept: "application/json",
    body: JSON.stringify({
      anthropic_version: "bedrock-2023-05-31",
      max_tokens: 4096,
      messages: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: `${systemPrompt}\n\n${userContent}`
            }
          ]
        }
      ]
    })
  });

    const result = await bedrockClient.send(command);

    const decoded = new TextDecoder().decode(result.body);
    return JSON.parse(decoded);
  }); // ✅ THIS closing bracket is critical

  const responseText = response.content?.[0]?.text || "";

  const inputTokens = response.usage?.input_tokens || 0;
  const outputTokens = response.usage?.output_tokens || 0;

  return {
    responseText,
    inputTokens,
    outputTokens
  };
}

/* ------------------------------------------------ */
/* MAIN PDF ANALYSIS ENTRY */
/* ------------------------------------------------ */

async function getDocumentAnalysis(s3Key) {
  console.log("Starting PDF analysis...");

  const pdfJson = await getJsonFromS3(s3Key);

  const fullText = analyzePdf(pdfJson);
  const textChunks = extractTextChunks(fullText);
  const batches = splitPdfIntoBatches(textChunks);

  let totalInputTokens = 0;
  let totalOutputTokens = 0;

  const results = [];

  for (const batch of batches) {
    const systemPrompt = `
You are a legal document audit assistant.
Analyze the following document chunk carefully and extract structured JSON output.
`;

    const { responseText, inputTokens, outputTokens } =
      await analyzeChunk(batch, systemPrompt);

    totalInputTokens += inputTokens;
    totalOutputTokens += outputTokens;

    try {
      results.push(JSON.parse(responseText));
    } catch (err) {
      console.warn("Failed to parse JSON from Claude response");
    }
  }

  const finalResult = mergeExtractionResults(results);

  return {
    result: finalResult,
    usage: {
      totalInputTokens,
      totalOutputTokens
    }
  };
}
export { getDocumentAnalysis };
export const analyzeDocument = getDocumentAnalysis;