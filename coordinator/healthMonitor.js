const axios = require('axios');

/**
 * HealthMonitor — pings every broker node every CHECK_INTERVAL_MS.
 * After FAILURE_THRESHOLD consecutive missed pings it fires onFailure(nodeName).
 * When a failed node responds again it fires onRecovery(nodeName).
 */
class HealthMonitor {
  constructor({ ring, onFailure, onRecovery, checkIntervalMs = 5000, failureThreshold = 3 }) {
    this.ring = ring;
    this.onFailure = onFailure;
    this.onRecovery = onRecovery;
    this.checkIntervalMs = checkIntervalMs;
    this.failureThreshold = failureThreshold;

    // nodeName → { status, failCount, lastCheck, lastSuccess, failedAt }
    this.nodeHealth = new Map();
    this._intervalId = null;
  }

  // ─── Initialise health records for all ring nodes ───────────────────────────
  init() {
    for (const nodeName of this.ring.getAllNodeNames()) {
      this.nodeHealth.set(nodeName, {
        status: 'HEALTHY',
        failCount: 0,
        lastCheck: null,
        lastSuccess: null,
        failedAt: null,
      });
    }
    console.log(`[HealthMonitor] Initialised for nodes: ${[...this.nodeHealth.keys()].join(', ')}`);
  }

  // ─── Start periodic checks ──────────────────────────────────────────────────
  start() {
    this.init();
    this._intervalId = setInterval(() => this._checkAll(), this.checkIntervalMs);
    console.log(`[HealthMonitor] Started — checking every ${this.checkIntervalMs / 1000}s, threshold=${this.failureThreshold}`);
  }

  stop() {
    if (this._intervalId) clearInterval(this._intervalId);
    console.log('[HealthMonitor] Stopped');
  }

  // ─── Ping every node ────────────────────────────────────────────────────────
  async _checkAll() {
    for (const [nodeName, health] of this.nodeHealth.entries()) {
      const url = this.ring.getNodeUrl(nodeName);
      if (!url) continue;

      health.lastCheck = new Date().toISOString();

      try {
        await axios.get(`${url}/health`, { timeout: 2000 });

        if (health.status === 'FAILED' || health.status === 'FAILED_OVER') {
          // Node came back
          console.log(`[HealthMonitor] Node ${nodeName} RECOVERED`);
          health.status = 'RECOVERED';
          health.failCount = 0;
          health.lastSuccess = new Date().toISOString();
          this.onRecovery(nodeName);
        } else {
          health.status = 'HEALTHY';
          health.failCount = 0;
          health.lastSuccess = new Date().toISOString();
        }
      } catch {
        health.failCount++;
        console.log(`[HealthMonitor] Node ${nodeName} check FAILED (${health.failCount}/${this.failureThreshold})`);

        if (health.failCount >= this.failureThreshold && health.status === 'HEALTHY') {
          health.status = 'FAILED';
          health.failedAt = new Date().toISOString();
          console.log(`[HealthMonitor] ⚠️  Node ${nodeName} declared FAILED — triggering failover`);
          this.onFailure(nodeName);
        }
      }
    }
  }

  // ─── Mark a node as failed-over (so recovery handler knows the context) ─────
  markFailedOver(nodeName) {
    const health = this.nodeHealth.get(nodeName);
    if (health) health.status = 'FAILED_OVER';
  }

  getStatus() {
    const result = {};
    for (const [nodeName, health] of this.nodeHealth.entries()) {
      result[nodeName] = { ...health };
    }
    return result;
  }
}

module.exports = HealthMonitor;
