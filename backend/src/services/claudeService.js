import AnthropicBedrock from '@anthropic-ai/bedrock-sdk';
import { getJsonFromS3 } from './s3Service.js';
import { 
  analyzePdf, 
  extractTextChunks, 
  splitPdfIntoBatches,
  mergeExtractionResults,
  PDF_CONFIG 
} from './pdfChunkService.js';

const anthropic = new AnthropicBedrock({
  awsRegion: process.env.AWS_REGION
});

// Rate limiting configuration
const RATE_LIMIT_CONFIG = {
  maxRetries: 5,
  baseDelayMs: 2000,      // Start with 2 second delay
  maxDelayMs: 120000,     // Max 2 minute delay
  backoffMultiplier: 2,   // Double delay each retry
  jitterMs: 1000          // Add random jitter up to 1 second
};

/**
 * Sleep for specified milliseconds
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Calculate delay with exponential backoff and jitter
 */
function calculateBackoffDelay(attempt) {
  const exponentialDelay = RATE_LIMIT_CONFIG.baseDelayMs * Math.pow(RATE_LIMIT_CONFIG.backoffMultiplier, attempt);
  const cappedDelay = Math.min(exponentialDelay, RATE_LIMIT_CONFIG.maxDelayMs);
  const jitter = Math.random() * RATE_LIMIT_CONFIG.jitterMs;
  return cappedDelay + jitter;
}

/**
 * Execute API call with retry logic for rate limits
 */
async function executeWithRetry(apiCall, context = 'API call') {
  let lastError;
  
  for (let attempt = 0; attempt < RATE_LIMIT_CONFIG.maxRetries; attempt++) {
    try {
      return await apiCall();
    } catch (error) {
      lastError = error;
      
      // Check if it's a rate limit error (429) or overloaded (529)
      const isRateLimited = error.status === 429 || error.status === 529;
      const isServerError = error.status >= 500 && error.status < 600;
      const isRetryable = isRateLimited || isServerError;
      
      if (!isRetryable || attempt === RATE_LIMIT_CONFIG.maxRetries - 1) {
        // Not retryable or last attempt - throw error
        throw error;
      }
      
      // Calculate delay
      let delayMs = calculateBackoffDelay(attempt);
      
      // Check for Retry-After header
      if (error.headers?.['retry-after']) {
        const retryAfter = parseInt(error.headers['retry-after'], 10);
        if (!isNaN(retryAfter)) {
          delayMs = Math.max(delayMs, retryAfter * 1000);
        }
      }
      
      console.log(`[Claude] ${context} - Rate limited (${error.status}). Attempt ${attempt + 1}/${RATE_LIMIT_CONFIG.maxRetries}. Waiting ${(delayMs / 1000).toFixed(1)}s before retry...`);
      
      await sleep(delayMs);
    }
  }
  
  throw lastError;
}

// Cache for master prompt to avoid repeated S3 calls
let cachedMasterPrompt = null;
let masterPromptCacheTime = 0;
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

async function getMasterPrompt() {
  const now = Date.now();
  if (cachedMasterPrompt && (now - masterPromptCacheTime) < CACHE_TTL) {
    return cachedMasterPrompt;
  }
  cachedMasterPrompt = await getJsonFromS3('masters/legal_audit_prompt.json');
  masterPromptCacheTime = now;
  return cachedMasterPrompt;
}

function buildSystemPrompt(masterPrompt, isPartialDocument = false, chunkInfo = null) {
  let contextInfo = '';
  
  if (isPartialDocument && chunkInfo) {
    contextInfo = `
IMPORTANT CONTEXT: You are analyzing PART of a larger document.
- This is chunk ${chunkInfo.chunkNumber} of ${chunkInfo.totalChunks}
- Pages: ${chunkInfo.startPage || 'N/A'} to ${chunkInfo.endPage || 'N/A'}
- Extract all information visible in THIS section. Other sections will be processed separately and merged.
- If you cannot find certain information in this section, use "Not in this section" for that field.
`;
  }

  return `${masterPrompt.systemRole}

You are analyzing legal documents (Title Search Reports, Encumbrance Certificates, Property TR Reports, etc.) for a bank/NBFC.
${contextInfo}
IMPORTANT: Respond ONLY with a valid JSON object. No markdown, no explanations, no code blocks.

The JSON must have these exact fields:
{
  "appl_no": "string - Reference/Application number from document",
  "borrower_name": "string - Name of borrower/mortgagor",
  "property_address": "string - Full property address with survey details",
  "property_type": "string - Residential/Commercial/Industrial/Agricultural",
  "state": "string - Indian state",
  "tsr_date": "string - Date of the report (DD-MMM-YYYY)",
  "ownership_title_chain_status": "string - Clean/Break/POA-based/Unregistered with details",
  "encumbrances_adverse_entries": "string - Yes/No with brief details",
  "subsequent_charges": "string - Yes/No with lender name & date if yes",
  "prior_charge_subsisting": "string - Yes/No with details",
  "roc_charge_flag": "string - Yes/No/NA/Unknown with charge ID if applicable",
  "litigation_lis_pendens": "string - Yes/No with case/court details",
  "mutation_status": "string - Done/Pending/Not Initiated/Unknown",
  "revenue_municipal_dues": "string - Yes/No/Unknown with amount if available",
  "land_use_zoning_status": "string - Compliant/Pending/Non-compliant with details",
  "stamping_registration_issues": "string - Yes/No with description",
  "mortgage_perfection_issues": "string - Yes/No with description",
  "advocate_adverse_remarks": "string - Yes/No with summary",
  "risk_rating": "string - High/Medium/Low",
  "enforceability_decision": "string - Enforceable/Enforceable with Conditions/Not Enforceable",
  "enforceability_rationale": "string - 2-3 lines explaining the decision",
  "recommended_actions": "string - Numbered list of actions",
  "confidence_score": "number - 0 to 100 indicating extraction confidence"
}

Risk Classification Rules:
HIGH RISK (any of):
${masterPrompt.riskClassification.high.map(r => `- ${r}`).join('\n')}

MEDIUM RISK (any of):
${masterPrompt.riskClassification.medium.map(r => `- ${r}`).join('\n')}

LOW RISK:
${masterPrompt.riskClassification.low.map(r => `- ${r}`).join('\n')}

If you cannot extract a field, use "Unknown" or "Not Available in Document".
If the document is unreadable or not a legal document, set confidence_score to 0 and risk_rating to "Manual Review Required".`;
}

function getFailedExtractionResult(errorMessage) {
  return {
    appl_no: 'EXTRACTION_FAILED',
    borrower_name: 'Unknown',
    property_address: 'Unknown',
    property_type: 'Unknown',
    state: 'Unknown',
    tsr_date: 'Unknown',
    ownership_title_chain_status: 'Unknown',
    encumbrances_adverse_entries: 'Unknown',
    subsequent_charges: 'Unknown',
    prior_charge_subsisting: 'Unknown',
    roc_charge_flag: 'Unknown',
    litigation_lis_pendens: 'Unknown',
    mutation_status: 'Unknown',
    revenue_municipal_dues: 'Unknown',
    land_use_zoning_status: 'Unknown',
    stamping_registration_issues: 'Unknown',
    mortgage_perfection_issues: 'Unknown',
    advocate_adverse_remarks: 'Unknown',
    risk_rating: 'Manual Review Required',
    enforceability_decision: 'Manual Review Required',
    enforceability_rationale: `Document processing failed: ${errorMessage}`,
    recommended_actions: '1. Manual review required\n2. Re-upload document if corrupted',
    confidence_score: 0
  };
}

/**
 * Analyze a single document chunk with Claude
 */
async function analyzeChunk(content, documentName, mediaType, chunkInfo = null, masterPrompt) {
  const isPartial = chunkInfo && chunkInfo.totalChunks > 1;
  const systemPrompt = buildSystemPrompt(masterPrompt, isPartial, chunkInfo);

  let userContent;
  
  if (mediaType === 'text') {
    userContent = [
      {
        type: 'text',
        text: `Analyze this legal document (${documentName})${isPartial ? ` - Part ${chunkInfo.chunkNumber}/${chunkInfo.totalChunks}` : ''} and extract all relevant information for legal risk assessment. Return ONLY a JSON object.\n\nDocument Content:\n${content}`
      }
    ];
  } else if (mediaType === 'pdf') {
    userContent = [
      {
        type: 'document',
        source: {
          type: 'base64',
          media_type: 'application/pdf',
          data: content
        }
      },
      {
        type: 'text',
        text: `Analyze this legal document (${documentName})${isPartial ? ` - Part ${chunkInfo.chunkNumber}/${chunkInfo.totalChunks}` : ''} and extract all relevant information for legal risk assessment. Return ONLY a JSON object.`
      }
    ];
  } else if (mediaType === 'multi-page-pdf') {
    // Multiple single-page PDFs in one request
    userContent = content.map((pageData, idx) => ({
      type: 'document',
      source: {
        type: 'base64',
        media_type: 'application/pdf',
        data: pageData.base64
      }
    }));
    userContent.push({
      type: 'text',
      text: `Analyze these ${content.length} pages from legal document (${documentName})${isPartial ? ` - Pages ${chunkInfo.startPage}-${chunkInfo.endPage}` : ''} and extract all relevant information for legal risk assessment. Return ONLY a JSON object combining information from all pages shown.`
    });
  } else if (mediaType === 'multi-image') {
    // Multiple images in one request
    userContent = content.map((imageData) => ({
      type: 'image',
      source: {
        type: 'base64',
        media_type: imageData.format === 'png' ? 'image/png' : 'image/jpeg',
        data: imageData.base64
      }
    }));
    userContent.push({
      type: 'text',
      text: `Analyze these ${content.length} pages from legal document (${documentName})${isPartial ? ` - Pages ${chunkInfo.startPage}-${chunkInfo.endPage}` : ''} and extract all relevant information for legal risk assessment. Return ONLY a JSON object combining information from all pages shown.`
    });
  } else {
    // Single image
    userContent = [
      {
        type: 'image',
        source: {
          type: 'base64',
          media_type: mediaType,
          data: content
        }
      },
      {
        type: 'text',
        text: `Analyze this legal document (${documentName})${isPartial ? ` - Part ${chunkInfo.chunkNumber}/${chunkInfo.totalChunks}` : ''} and extract all relevant information for legal risk assessment. Return ONLY a JSON object.`
      }
    ];
  }

  // Execute API call with retry logic for rate limits
  const response = await executeWithRetry(
    () => anthropic.messages.create({
      model: 'anthropic.claude-sonnet-4-20250514-v1:0',
      max_tokens: 4096,
      messages: [{ role: 'user', content: userContent }],
      system: systemPrompt
    }),
    `Analyzing ${documentName}${chunkInfo ? ` (chunk ${chunkInfo.chunkNumber}/${chunkInfo.totalChunks})` : ''}`
  );

  const responseText = response.content[0].text;
  
  // Extract token usage
  const inputTokens = response.usage.input_tokens;
  const outputTokens = response.usage.output_tokens;
  const totalTokens = inputTokens + outputTokens;
  
  console.log(`[Claude] Tokens - Input: ${inputTokens.toLocaleString()}, Output: ${outputTokens.toLocaleString()}, Total: ${totalTokens.toLocaleString()}`);
  
  // Parse JSON from response
  let result;
  try {
    result = JSON.parse(responseText);
  } catch {
    const jsonMatch = responseText.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      result = JSON.parse(jsonMatch[0]);
    } else {
      throw new Error('No valid JSON found in response');
    }
  }

  return {
    success: true,
    data: result,
    tokensUsed: totalTokens,
    tokenDetails: {
      input: inputTokens,
      output: outputTokens
    }
  };
}

/**
 * Main document analysis function - handles both small and large files
 */
export async function analyzeDocument(documentContent, documentName, mediaType = 'text', options = {}) {
  const { 
    onProgress = null,  // Callback for progress updates
    pdfBuffer = null    // Original PDF buffer for large file processing
  } = options;

  let masterPrompt;
  try {
    masterPrompt = await getMasterPrompt();
  } catch (promptError) {
    console.error('[Claude] Failed to load master prompt:', promptError);
    return {
      success: false,
      error: `Failed to load master prompt: ${promptError.message}`,
      data: getFailedExtractionResult(`Master prompt error: ${promptError.message}`),
      tokenDetails: { input: 0, output: 0 }
    };
  }
  
  try {
    // For non-PDF or small content, process directly
    if (mediaType === 'text' || mediaType !== 'pdf' || !pdfBuffer) {
      console.log(`[Claude] Processing ${documentName} with mediaType: ${mediaType}`);
      const result = await analyzeChunk(documentContent, documentName, mediaType, null, masterPrompt);
      // Add token details to result
      result.tokenDetails = result.tokenDetails || { input: 0, output: 0 };
      return result;
    }

    // For PDFs, check if we need to chunk
    const fileSize = pdfBuffer.length;
    
    if (fileSize <= PDF_CONFIG.MAX_DIRECT_PDF_SIZE) {
      // Small PDF - process directly
      console.log(`[Claude] Processing ${documentName} directly (${(fileSize / 1024 / 1024).toFixed(1)}MB)`);
      const result = await analyzeChunk(documentContent, documentName, 'pdf', null, masterPrompt);
      result.tokenDetails = result.tokenDetails || { input: 0, output: 0 };
      return result;
    }

    // Large PDF - need to chunk
    console.log(`[Claude] Large PDF detected (${(fileSize / 1024 / 1024).toFixed(1)}MB), analyzing for chunking strategy...`);
    
    const analysis = await analyzePdf(pdfBuffer);
    console.log(`[Claude] PDF Analysis:`, JSON.stringify(analysis, null, 2));
    
    if (onProgress) {
      onProgress({
        phase: 'analyzing',
        message: `Large PDF (${analysis.fileSizeMB}MB, ${analysis.pageCount} pages) - using ${analysis.strategy} strategy`,
        strategy: analysis.strategy
      });
    }

    return await processLargePdf(pdfBuffer, documentName, analysis, masterPrompt, onProgress);

  } catch (error) {
    console.error('[Claude] API error details:', {
      name: error.name,
      message: error.message,
      status: error.status,
      statusCode: error.statusCode,
      code: error.code
    });
    
    // Check for specific error types
    let errorMessage = error.message;
    if (error.status === 429) {
      errorMessage = 'Rate limit exceeded - too many requests';
    } else if (error.status === 401) {
      errorMessage = 'Invalid API key';
    } else if (error.status === 400) {
      errorMessage = `Bad request: ${error.message}`;
    } else if (error.code === 'ECONNREFUSED' || error.code === 'ENOTFOUND') {
      errorMessage = 'Network error - cannot reach Claude API';
    }
    
    return {
      success: false,
      error: errorMessage,
      data: getFailedExtractionResult(errorMessage),
      tokenDetails: { input: 0, output: 0 }
    };
  }
}

/**
 * Process large PDFs using chunking strategies
 */
async function processLargePdf(pdfBuffer, documentName, analysis, masterPrompt, onProgress) {
  const { strategy, pageCount } = analysis;
  const results = [];
  let totalInputTokens = 0;
  let totalOutputTokens = 0;

  try {
    switch (strategy) {
      case 'text-chunk': {
        // Extract text and process in chunks
        console.log(`[Claude] Using text-chunk strategy for ${documentName}`);
        const textChunks = await extractTextChunks(pdfBuffer);
        
        if (textChunks.length === 0) {
          // Fallback to page-split if no text extracted
          console.log(`[Claude] No text extracted, falling back to page-split strategy`);
          return await processLargePdf(pdfBuffer, documentName, { ...analysis, strategy: 'page-split' }, masterPrompt, onProgress);
        }

        console.log(`[Claude] Processing ${textChunks.length} text chunks...`);
        
        for (let i = 0; i < textChunks.length; i++) {
          const chunk = textChunks[i];
          
          if (onProgress) {
            onProgress({
              phase: 'processing',
              message: `Processing text chunk ${i + 1}/${textChunks.length}`,
              current: i + 1,
              total: textChunks.length,
              tokensUsed: { input: totalInputTokens, output: totalOutputTokens }
            });
          }

          try {
            const result = await analyzeChunk(
              chunk.content,
              documentName,
              'text',
              {
                chunkNumber: i + 1,
                totalChunks: textChunks.length
              },
              masterPrompt
            );

            if (result.success) {
              results.push(result.data);
              totalInputTokens += result.tokenDetails?.input || 0;
              totalOutputTokens += result.tokenDetails?.output || 0;
            }

            // Rate limiting
            await new Promise(resolve => setTimeout(resolve, 500));
          } catch (chunkError) {
            console.error(`[Claude] Error processing chunk ${i + 1}:`, chunkError.message);
          }
        }
        break;
      }

      case 'page-split': {
        // Split PDF into batches and process each batch
        console.log(`[Claude] Using page-split strategy for ${documentName}`);
        const batches = await splitPdfIntoBatches(pdfBuffer);
        
        console.log(`[Claude] Processing ${batches.length} page batches...`);

        for (let i = 0; i < batches.length; i++) {
          const batch = batches[i];
          
          if (onProgress) {
            onProgress({
              phase: 'processing',
              message: `Processing pages ${batch.startPage}-${batch.endPage} (batch ${i + 1}/${batches.length})`,
              current: i + 1,
              total: batches.length,
              tokensUsed: { input: totalInputTokens, output: totalOutputTokens }
            });
          }

          try {
            const result = await analyzeChunk(
              batch.base64,
              documentName,
              'pdf',
              {
                chunkNumber: i + 1,
                totalChunks: batches.length,
                startPage: batch.startPage,
                endPage: batch.endPage
              },
              masterPrompt
            );

            if (result.success) {
              results.push(result.data);
              totalInputTokens += result.tokenDetails?.input || 0;
              totalOutputTokens += result.tokenDetails?.output || 0;
              console.log(`[Claude] Batch ${i + 1}/${batches.length} complete - Risk: ${result.data.risk_rating}, Tokens: +${result.tokenDetails?.input || 0}/${result.tokenDetails?.output || 0}`);
            }

            // Rate limiting between batches (longer wait for stability)
            await new Promise(resolve => setTimeout(resolve, 1500));
          } catch (batchError) {
            console.error(`[Claude] Error processing batch ${i + 1}:`, batchError.message);
            // Continue with next batch instead of failing completely
          }
        }
        break;
      }

      default:
        throw new Error(`Unknown processing strategy: ${strategy}`);
    }

    // Merge results from all chunks
    if (results.length === 0) {
      return {
        success: false,
        error: 'No chunks processed successfully',
        data: getFailedExtractionResult('All chunks failed to process'),
        tokenDetails: { input: totalInputTokens, output: totalOutputTokens }
      };
    }

    const totalTokens = totalInputTokens + totalOutputTokens;
    console.log(`[Claude] Merging ${results.length} chunk results... Total tokens: ${totalTokens.toLocaleString()} (${totalInputTokens.toLocaleString()} in / ${totalOutputTokens.toLocaleString()} out)`);
    const mergedResult = mergeExtractionResults(results, { documentName, pageCount });

    if (onProgress) {
      onProgress({
        phase: 'complete',
        message: `Processed ${results.length} chunks, merged into final result`,
        risk: mergedResult.risk_rating,
        confidence: mergedResult.confidence_score,
        tokensUsed: { input: totalInputTokens, output: totalOutputTokens, total: totalTokens }
      });
    }

    return {
      success: true,
      data: mergedResult,
      tokensUsed: totalTokens,
      tokenDetails: { input: totalInputTokens, output: totalOutputTokens },
      chunksProcessed: results.length,
      strategy: strategy
    };

  } catch (error) {
    console.error('[Claude] Error in processLargePdf:', error);
    return {
      success: false,
      error: error.message,
      data: getFailedExtractionResult(error.message),
      tokenDetails: { input: totalInputTokens, output: totalOutputTokens }
    };
  }
}

/**
 * Analyze PDF and return processing strategy (for UI preview)
 */
export async function getDocumentAnalysis(pdfBuffer, documentName) {
  try {
    const analysis = await analyzePdf(pdfBuffer);
    return {
      success: true,
      documentName,
      ...analysis
    };
  } catch (error) {
    return {
      success: false,
      error: error.message,
      documentName
    };
  }
}
