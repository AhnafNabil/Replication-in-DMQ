const express = require('express');
const axios = require('axios');
const HashRing = require('./hashRing');
const HealthMonitor = require('./healthMonitor');

const app = express();
app.use(express.json());

const REPLICATION_FACTOR = parseInt(process.env.REPLICATION_FACTOR || '3');
const PORT = 7000;

// ─── Build Hash Ring from environment ────────────────────────────────────────
// BROKER_NODES format: "node-a=http://node-a:5000,node-b=http://node-b:5000,..."
const ring = new HashRing(150);

const brokerNodesEnv = process.env.BROKER_NODES || '';
brokerNodesEnv.split(',').forEach(entry => {
  const [name, url] = entry.trim().split('=');
  if (name && url) {
    ring.addNode(name, url);
    console.log(`[Coordinator] Registered broker: ${name} → ${url}`);
  }
});

// ─── Failover State ───────────────────────────────────────────────────────────
// Tracks failover events and which node is the current primary after promotion
const failoverEvents = [];
// nodeName → promoted replica name (used for routing after failover)
const promotedNodes = new Map();

// ─── Health Monitor ──────────────────────────────────────────────────────────
const monitor = new HealthMonitor({
  ring,
  checkIntervalMs: 5000,
  failureThreshold: 3,

  onFailure: async (failedNode) => {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`[Coordinator] 🔴 FAILOVER TRIGGERED for ${failedNode}`);
    console.log(`${'='.repeat(60)}`);

    // Find the replica that should be promoted (first healthy clockwise node)
    // We use a dummy key known to map to the failed node, but in practice we
    // promote the first replica in the replication group of the failed node.
    // Strategy: look up any key that has failedNode as primary.
    // Simpler: promote the next node in the ring after failedNode.

    const allNodes = ring.getAllNodeNames();
    const failedIndex = allNodes.indexOf(failedNode);
    let promotedNode = null;

    // Walk clockwise through node list to find a healthy, non-failed node
    for (let i = 1; i < allNodes.length; i++) {
      const candidate = allNodes[(failedIndex + i) % allNodes.length];
      const candidateUrl = ring.getNodeUrl(candidate);
      try {
        await axios.get(`${candidateUrl}/health`, { timeout: 2000 });
        promotedNode = candidate;
        break;
      } catch {
        console.log(`[Coordinator] Candidate ${candidate} also unhealthy, skipping`);
      }
    }

    if (!promotedNode) {
      console.log(`[Coordinator] 🚨 No healthy replica found for ${failedNode}. System degraded.`);
      return;
    }

    // Tell the promoted node it is now primary
    try {
      await axios.post(`${ring.getNodeUrl(promotedNode)}/promote`);
    } catch (err) {
      console.log(`[Coordinator] Failed to send promote signal: ${err.message}`);
    }

    promotedNodes.set(failedNode, promotedNode);
    monitor.markFailedOver(failedNode);

    const event = {
      failedNode,
      promotedNode,
      timestamp: new Date().toISOString(),
    };
    failoverEvents.push(event);

    console.log(`[Coordinator] ✅ ${promotedNode} promoted as new primary for ${failedNode}'s partitions`);
    console.log(`${'='.repeat(60)}\n`);
  },

  onRecovery: (recoveredNode) => {
    console.log(`[Coordinator] 🟢 ${recoveredNode} has recovered.`);
    console.log(`[Coordinator] Keeping current topology — ${promotedNodes.get(recoveredNode) || 'original'} remains primary.`);
  },
});

monitor.start();

// ─── Helper: resolve effective primary for a node ────────────────────────────
function resolveEffectivePrimary(primary) {
  // If this primary has failed over, return the promoted replacement
  return promotedNodes.get(primary) || primary;
}

// ─── Routes ──────────────────────────────────────────────────────────────────

/**
 * POST /produce
 * Body: { key, payload }
 * Routes message to the correct primary broker and triggers replication.
 */
app.post('/produce', async (req, res) => {
  const { key, payload } = req.body;

  if (!key || payload === undefined) {
    return res.status(400).json({ error: 'key and payload are required' });
  }

  const { primary: rawPrimary, replicas: rawReplicas, keyHash } = ring.getNodesForKey(key, REPLICATION_FACTOR);
  const primary = resolveEffectivePrimary(rawPrimary);
  const replicas = rawReplicas.map(resolveEffectivePrimary).filter(n => n !== primary);

  const primaryUrl = ring.getNodeUrl(primary);
  const replicaUrls = replicas.map(n => ring.getNodeUrl(n)).filter(Boolean);

  console.log(`\n[Coordinator] PRODUCE key="${key}"`);
  console.log(`  Key Hash  : ${keyHash}`);
  console.log(`  Primary   : ${primary} (${primaryUrl})`);
  console.log(`  Replicas  : ${replicas.join(', ')}`);

  try {
    const response = await axios.post(`${primaryUrl}/store`, {
      key,
      payload,
      replicateTo: replicaUrls,
    });

    res.status(201).json({
      success: true,
      key,
      keyHash,
      primary,
      replicas,
      replicationResults: response.data.replicationResults,
    });
  } catch (err) {
    console.log(`[Coordinator] Primary ${primary} unreachable: ${err.message}`);
    res.status(503).json({ success: false, error: `Primary broker ${primary} is unreachable`, key });
  }
});

/**
 * GET /consume/:key
 * Reads a message from the primary (falls back to replica if primary is down).
 */
app.get('/consume/:key', async (req, res) => {
  const { key } = req.params;
  const { primary: rawPrimary, replicas: rawReplicas } = ring.getNodesForKey(key, REPLICATION_FACTOR);
  const primary = resolveEffectivePrimary(rawPrimary);
  const replicas = rawReplicas.map(resolveEffectivePrimary);

  // Try primary first, then each replica
  const candidates = [primary, ...replicas];

  for (const nodeName of candidates) {
    const url = ring.getNodeUrl(nodeName);
    if (!url) continue;

    try {
      const response = await axios.get(`${url}/fetch/${key}`, { timeout: 2000 });
      if (response.data.success) {
        const failover = nodeName !== primary;
        console.log(`[Coordinator] CONSUME key="${key}" → served by ${nodeName}${failover ? ' (FAILOVER)' : ''}`);
        return res.json({
          ...response.data,
          servedBy: nodeName,
          failover: promotedNodes.size > 0,
          source: nodeName === rawPrimary ? 'primary' : 'replica',
        });
      }
    } catch {
      console.log(`[Coordinator] Node ${nodeName} unreachable when consuming key="${key}", trying next...`);
    }
  }

  res.status(404).json({ success: false, key, error: 'Message not found on any node' });
});

/**
 * GET /ring
 * Shows the current state of the hash ring.
 */
app.get('/ring', (req, res) => {
  res.json(ring.getRingInfo());
});

/**
 * GET /route/:key
 * Shows where a given key would be routed without actually producing/consuming.
 */
app.get('/route/:key', (req, res) => {
  const { key } = req.params;
  const { primary: rawPrimary, replicas: rawReplicas, keyHash } = ring.getNodesForKey(key, REPLICATION_FACTOR);
  const primary = resolveEffectivePrimary(rawPrimary);
  const replicas = rawReplicas.map(resolveEffectivePrimary);

  res.json({
    key,
    keyHash,
    primary,
    primaryUrl: ring.getNodeUrl(primary),
    replicas,
    replicaUrls: replicas.map(n => ring.getNodeUrl(n)),
    failoverActive: promotedNodes.size > 0,
  });
});

/**
 * GET /health/nodes
 * Shows health status of all broker nodes.
 */
app.get('/health/nodes', (req, res) => {
  res.json(monitor.getStatus());
});

/**
 * GET /failover/status
 * Shows failover history.
 */
app.get('/failover/status', (req, res) => {
  res.json({
    totalFailovers: failoverEvents.length,
    activePromotions: Object.fromEntries(promotedNodes),
    events: failoverEvents,
  });
});

/**
 * GET /health
 * Coordinator self-health check.
 */
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', service: 'coordinator', timestamp: new Date().toISOString() });
});

// ─── Start ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\n${'='.repeat(60)}`);
  console.log('COORDINATOR SERVICE STARTED');
  console.log(`${'='.repeat(60)}`);
  console.log(`Port             : ${PORT}`);
  console.log(`Replication Factor: ${REPLICATION_FACTOR}`);
  console.log(`Brokers          : ${ring.getAllNodeNames().join(', ')}`);
  console.log(`${'='.repeat(60)}\n`);
});
