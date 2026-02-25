const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

const NODE_ID = process.env.NODE_ID || 'node-unknown';
const PORT = 5000;

// ─── In-Memory State ──────────────────────────────────────────────────────────
const messageStore = {};   // { key: { payload, timestamp } }
const replicationLog = []; // audit trail of all replication events

// ─── Helper ───────────────────────────────────────────────────────────────────
function log(msg) {
  console.log(`[${NODE_ID}] ${new Date().toISOString()} - ${msg}`);
}

// ─── Routes ───────────────────────────────────────────────────────────────────

// Called by coordinator to store a message as PRIMARY
app.post('/store', async (req, res) => {
  const { key, payload, replicateTo = [] } = req.body;

  if (!key || payload === undefined) {
    return res.status(400).json({ error: 'key and payload are required' });
  }

  // 1. Save locally
  messageStore[key] = { payload, timestamp: new Date().toISOString(), role: 'primary' };
  log(`Stored PRIMARY key="${key}"`);

  // 2. Replicate to replica nodes
  const replicationResults = [];
  for (const replicaUrl of replicateTo) {
    try {
      await axios.post(`${replicaUrl}/replicate`, { key, payload, primaryNode: NODE_ID });
      replicationResults.push({ node: replicaUrl, status: 'success' });
      log(`Replicated key="${key}" to ${replicaUrl}`);
    } catch (err) {
      replicationResults.push({ node: replicaUrl, status: 'failed', error: err.message });
      log(`FAILED to replicate key="${key}" to ${replicaUrl}: ${err.message}`);
    }
  }

  // 3. Add to replication log
  replicationLog.push({
    event: 'stored_as_primary',
    key,
    payload,
    replicatedTo: replicateTo,
    replicationResults,
    timestamp: new Date().toISOString(),
  });

  res.status(201).json({
    success: true,
    node: NODE_ID,
    role: 'primary',
    key,
    replicatedTo: replicateTo,
    replicationResults,
  });
});

// Called by PRIMARY broker to store a replica copy
app.post('/replicate', (req, res) => {
  const { key, payload, primaryNode } = req.body;

  if (!key || payload === undefined) {
    return res.status(400).json({ error: 'key and payload are required' });
  }

  messageStore[key] = { payload, timestamp: new Date().toISOString(), role: 'replica', replicaOf: primaryNode };
  log(`Stored REPLICA key="${key}" (primary: ${primaryNode})`);

  replicationLog.push({
    event: 'stored_as_replica',
    key,
    payload,
    receivedFrom: primaryNode,
    timestamp: new Date().toISOString(),
  });

  res.json({ success: true, node: NODE_ID, role: 'replica', key });
});

// Called by coordinator to fetch a message (for consumer)
app.get('/fetch/:key', (req, res) => {
  const { key } = req.params;
  const entry = messageStore[key];

  if (!entry) {
    return res.status(404).json({ success: false, node: NODE_ID, key, reason: 'not_found' });
  }

  res.json({ success: true, node: NODE_ID, key, ...entry });
});

// Called by coordinator's health monitor
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    node: NODE_ID,
    messageCount: Object.keys(messageStore).length,
    timestamp: new Date().toISOString(),
  });
});

// Show everything stored on this node (for debugging/visualization)
app.get('/messages', (req, res) => {
  res.json({
    node: NODE_ID,
    messageCount: Object.keys(messageStore).length,
    messages: messageStore,
  });
});

// Show full replication audit log
app.get('/log', (req, res) => {
  const primaryEntries = replicationLog.filter(e => e.event === 'stored_as_primary');
  const replicaEntries = replicationLog.filter(e => e.event === 'stored_as_replica');

  res.json({
    node: NODE_ID,
    summary: {
      totalPrimaryKeys: primaryEntries.length,
      totalReplicaKeys: replicaEntries.length,
    },
    storedAsPrimary: primaryEntries,
    storedAsReplica: replicaEntries,
  });
});

// Mark node as writable primary (called during failover promotion)
app.post('/promote', (req, res) => {
  log('Promoted to PRIMARY (failover)');
  // All replica entries become primary entries in the log
  replicationLog.push({
    event: 'promoted_to_primary',
    timestamp: new Date().toISOString(),
  });
  res.json({ success: true, node: NODE_ID, message: 'Node promoted to primary' });
});

// ─── Start ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  log(`Broker node listening on port ${PORT}`);
});
