const express = require('express');
const axios = require('axios');
const path = require('path');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Server-Sent Events (SSE) subscribers for real-time frontend logs
let sseClients = [];

function broadcastLog(data) {
  sseClients.forEach((client) => {
    client.res.write(`data: ${JSON.stringify(data)}\n\n`);
  });
}

app.get('/api/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const clientId = Date.now();
  const newClient = { id: clientId, res };
  sseClients.push(newClient);

  req.on('close', () => {
    sseClients = sseClients.filter((c) => c.id !== clientId);
  });
});

// Helper function to delay execution (6.67s = 9 requests/min)
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Normalizes Kenyan phone numbers to standard international format (2547...)
function normalizePhone(phone) {
  let cleaned = phone.replace(/\D/g, '');
  if (cleaned.startsWith('0')) {
    cleaned = '254' + cleaned.slice(1);
  } else if (cleaned.startsWith('7') || cleaned.startsWith('1')) {
    cleaned = '254' + cleaned;
  }
  return cleaned;
}

app.post('/api/bulk-stk', async (req, res) => {
  const { numbersText, amount, reference } = req.body;

  if (!numbersText || !amount) {
    return res.status(400).json({ error: 'Missing numbers or amount.' });
  }

  // Parse numbers line-by-line or comma-separated
  const rawNumbers = numbersText.split(/[\n,]+/).map((n) => n.trim()).filter(Boolean);
  const validNumbers = rawNumbers.map(normalizePhone).filter((n) => /^254\d{9}$/.test(n));

  if (validNumbers.length === 0) {
    return res.status(400).json({ error: 'No valid phone numbers found.' });
  }

  // Acknowledge request to client while queue runs asynchronously in background
  res.json({
    message: 'Bulk processing initiated',
    totalCount: validNumbers.length,
    estimatedMinutes: ((validNumbers.length * 6.67) / 60).toFixed(1)
  });

  // Execute job asynchronously with rate limiting (9 requests / minute)
  (async () => {
    broadcastLog({
      type: 'START',
      message: `Starting bulk run for ${validNumbers.length} numbers. Rate: 9 requests/min.`,
      total: validNumbers.length
    });

    const INTERVAL_MS = 6670; // 60,000ms / 9 = ~6670ms spacing

    for (let i = 0; i < validNumbers.length; i++) {
      const phone = validNumbers[i];
      const startTime = Date.now();

      try {
        const payload = {
          phone_number: phone,
          amount: Number(amount),
          channel_id: process.env.LIPWA_CHANNEL_ID || 'CH_EIU3939',
          callback_url: process.env.LIPWA_CALLBACK_URL || 'https://callback.com',
          api_ref: {
            name: reference || 'Bulk Payment',
            email: 'bulk@system.local'
          }
        };

        const response = await axios.post('https://pay.lipwa.app/api/payments', payload, {
          headers: {
            Authorization: `Bearer ${process.env.LIPWA_API_KEY}`,
            'Content-Type': 'application/json'
          },
          timeout: 10000
        });

        broadcastLog({
          type: 'SUCCESS',
          index: i + 1,
          total: validNumbers.length,
          phone,
          status: 'SUCCESS',
          data: response.data
        });
      } catch (err) {
        broadcastLog({
          type: 'FAILURE',
          index: i + 1,
          total: validNumbers.length,
          phone,
          status: 'FAILED',
          error: err.response ? err.response.data : err.message
        });
      }

      // Maintain rate limit (except after last element)
      if (i < validNumbers.length - 1) {
        const elapsed = Date.now() - startTime;
        const waitTime = Math.max(0, INTERVAL_MS - elapsed);
        await sleep(waitTime);
      }
    }

    broadcastLog({ type: 'COMPLETE', message: 'Bulk processing queue finished.' });
  })();
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
