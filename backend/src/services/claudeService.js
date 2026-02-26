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

export async function analyzeDocument(documentContent, documentName, mediaType = 'text', options = {}) {
  const systemPrompt = `You are a legal document audit assistant for a bank/NBFC.
Analyze this legal document and extract structured information.
Return ONLY a valid JSON object with these exact fields:
{
  "appl_no": "string",
  "borrower_name": "string", 
  "property_address": "string",
  "property_type": "string",
  "state": "string",
  "tsr_date": "string",
  "ownership_title_chain_status": "string",
  "encumbrances_adverse_entries": "string",
  "subsequent_charges": "string",
  "prior_charge_subsisting": "string",
  "roc_charge_flag": "string",
  "litigation_lis_pendens": "string",
  "mutation_status": "string",
  "revenue_municipal_dues": "string",
  "land_use_zoning_status": "string",
  "stamping_registration_issues": "string",
  "mortgage_perfection_issues": "string",
  "advocate_adverse_remarks": "string",
  "risk_rating": "High|Medium|Low",
  "enforceability_decision": "string",
  "enforceability_rationale": "string",
  "recommended_actions": "string",
  "confidence_score": 0.0
}
No markdown. No explanation. Just JSON.`;

  try {
    const { responseText, inputTokens, outputTokens } = await analyzeChunk(documentContent, systemPrompt);

    let data;
    try {
      data = JSON.parse(responseText);
    } catch {
      const jsonMatch = responseText.match(/\{[\s\S]*\}/);
      data = jsonMatch ? JSON.parse(jsonMatch[0]) : null;
    }

    return {
      success: !!data,
      data,
      tokenDetails: { input: inputTokens, output: outputTokens }
    };

  } catch (error) {
    console.error('[Bedrock] analyzeDocument error:', error.message);
    return {
      success: false,
      error: error.message,
      data: null,
      tokenDetails: { input: 0, output: 0 }
    };
  }
}

export async function getDocumentAnalysis(pdfBuffer, documentName) {
  return { success: true, documentName };
}