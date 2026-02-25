import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { loadSecrets } from './config/secrets.js';
import authRoutes from './routes/auth.js';
import mastersRoutes from './routes/masters.js';
import jobsRoutes from './routes/jobs.js';
import { initializeS3Bucket } from './services/s3Service.js';
import { initializeQueue } from './services/queueService.js';
import { initializeWorker } from './workers/documentWorker.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors({
  origin: process.env.FRONTEND_URL || '*',
  credentials: true
}));

// Increase body parser limits for large JSON payloads
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Routes
app.use('/api/auth', authRoutes);
app.use('/api/masters', mastersRoutes);
app.use('/api/jobs', jobsRoutes);

// Error handler
app.use((err, req, res, next) => {
  console.error('Error:', err);
  
  if (err.code === 'LIMIT_FILE_SIZE') {
    return res.status(413).json({ error: 'File too large. Maximum size is 4GB.' });
  }
  
  res.status(err.status || 500).json({
    error: err.message || 'Internal server error',
    details: process.env.NODE_ENV === 'development' ? err.stack : undefined
  });
});

// Initialize and start
async function start() {
  try {
    // Load secrets from AWS Secrets Manager FIRST (production only)
    // In local/dev it skips this and uses .env file
    await loadSecrets();

    // Initialize Redis queue
    console.log('Initializing queue...');
    const queueResult = await initializeQueue();
    if (queueResult.useInMemory) {
      console.log('✓ Using in-memory queue');
    } else {
      console.log('✓ Redis queue initialized');
    }

    // Initialize S3 bucket structure
    await initializeS3Bucket();
    console.log('✓ S3 bucket initialized');

    // Initialize BullMQ worker
    await initializeWorker();
    console.log('✓ Document processing worker initialized');

    app.listen(PORT, () => {
      console.log(`✓ Server running on port ${PORT}`);
      console.log(`  Environment: ${process.env.NODE_ENV || 'development'}`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

start();