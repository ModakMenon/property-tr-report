# Legal Audit Platform

AI-powered legal document audit system for NBFC/Banks to process Title Search Reports (TSRs), Encumbrance Certificates, and Property TR Reports at scale.

## Features

- **Bulk Document Processing**: Upload up to 1500+ documents via ZIP file
- **Large PDF Support**: Process single PDFs up to 500MB with intelligent chunking
- **AI-Powered Analysis**: Claude API extracts and analyzes legal risk factors
- **Real-time Progress**: Live commentary during document processing
- **Risk Classification**: Automatic High/Medium/Low risk categorization
- **Excel Reports**: Comprehensive output with Detail and Summary tabs
- **Manual Review Queue**: Failed documents flagged for human review

## Large PDF Processing

The platform automatically handles large PDFs (>20MB) using intelligent chunking strategies:

### Processing Strategies

| Strategy | When Used | How It Works |
|----------|-----------|--------------|
| `direct-text` | Small text-based PDFs (<20MB) | Extract text, send to Claude |
| `direct-pdf` | Small scanned PDFs (<20MB) | Send PDF directly to Claude |
| `text-chunk` | Large text-based PDFs | Extract text, split into chunks, process each, merge results |
| `page-images` | Large scanned PDFs | Split into pages, process batches of 10 pages, merge results |

### Configuration (in pdfChunkService.js)

```javascript
PDF_CONFIG = {
  MAX_DIRECT_PDF_SIZE: 20 * 1024 * 1024,  // 20MB threshold
  MAX_PAGES_PER_BATCH: 10,                 // Pages per API call
  IMAGE_DPI: 200,                          // For image conversion
  MAX_TOTAL_PAGES: 500                     // Safety limit
}
```

### Result Merging

When processing chunked documents, results are intelligently merged:
- **Risk Rating**: Takes the highest risk found across chunks
- **Enforceability**: Takes the most restrictive decision
- **Yes/No Fields**: If any chunk says "Yes", final result is "Yes"
- **Text Fields**: Concatenated with semicolons
- **Confidence Score**: Averaged across all chunks

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   React Frontend │────▶│  Node.js API    │────▶│   AWS S3        │
│   (Render.com)  │     │  (Render.com)   │     │   Storage       │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │ Upstash  │ │ Claude   │ │ BullMQ   │
              │ Redis    │ │ API      │ │ Workers  │
              └──────────┘ └──────────┘ └──────────┘
```

## Tech Stack

- **Frontend**: React 18, Tailwind CSS, Vite
- **Backend**: Node.js, Express
- **Queue**: Upstash Redis + BullMQ
- **Storage**: AWS S3
- **AI**: Claude API (claude-sonnet-4-20250514)
- **PDF Processing**: pdf-lib, pdf-parse, pdf2pic
- **Deployment**: Render.com

## Environment Variables

### Backend (.env)
```
PORT=3001
NODE_ENV=production

# AWS S3
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=ap-south-1
S3_BUCKET_NAME=legal-audit-platform

# Upstash Redis
UPSTASH_REDIS_URL=your_upstash_url

# Claude API
ANTHROPIC_API_KEY=your_anthropic_key

# Super Admin
SUPER_ADMIN_EMAIL=nilesh@yourcompany.com
```

### Frontend (.env)
```
VITE_API_URL=https://your-backend.onrender.com
```

## Local Development

### Backend
```bash
cd backend
npm install
npm run dev
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

## Deployment (Render.com)

1. Push to GitHub
2. Create Backend Web Service:
   - Build: `cd backend && npm install`
   - Start: `cd backend && npm start`
3. Create Frontend Static Site:
   - Build: `cd frontend && npm install && npm run build`
   - Publish: `frontend/dist`

## S3 Bucket Structure

```
s3://legal-audit-platform/
├── masters/
│   └── legal_audit_prompt.json
├── jobs/
│   └── {date}_{timestamp}_{jobId}/
│       ├── uploads/
│       │   ├── raw/
│       │   └── extracted/
│       ├── processing/
│       │   ├── queue.json
│       │   ├── results/
│       │   └── failed/
│       └── output/
│           └── Legal_Audit_Report.xlsx
└── users/
    └── users.json
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/auth/login | Mock SSO login |
| GET | /api/masters/prompt | Get audit prompt |
| PUT | /api/masters/prompt | Update audit prompt |
| POST | /api/jobs/create | Create new job |
| POST | /api/jobs/:id/upload | Upload ZIP documents |
| POST | /api/jobs/:id/upload-pdf | Upload single PDF (supports large files) |
| GET | /api/jobs/:id/analyze-pdf | Get PDF analysis/strategy preview |
| POST | /api/jobs/:id/extract | Start extraction |
| POST | /api/jobs/:id/analyze | Start analysis |
| GET | /api/jobs/:id/status | Get job status |
| GET | /api/jobs/:id/events | SSE for live updates |
| GET | /api/jobs/:id/download | Download report |
| GET | /api/jobs | List all jobs |

## License

Proprietary - Applied Cloud Computing (ACC)
