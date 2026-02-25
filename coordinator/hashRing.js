const crypto = require('crypto');

/**
 * Consistent Hash Ring with Virtual Nodes.
 *
 * Each physical node is placed at VIRTUAL_NODES positions on a circular
 * hash space (0 → 2^32 - 1). A key is owned by the first node clockwise
 * from hash(key). Replicas are the next N-1 nodes clockwise after the primary.
 */
class HashRing {
  constructor(virtualNodeCount = 150) {
    this.virtualNodeCount = virtualNodeCount;
    this.hashSpace = Math.pow(2, 32);

    // position → nodeName
    this.ring = new Map();
    // sorted list of all virtual node positions
    this.sortedPositions = [];
    // nodeName → { url, virtualPositions[] }
    this.nodes = new Map();
  }

  // ─── SHA-256 hash collapsed to 32-bit integer ───────────────────────────────
  hash(key) {
    const hex = crypto.createHash('sha256').update(key).digest('hex');
    return parseInt(hex.substring(0, 8), 16) % this.hashSpace;
  }

  // ─── Add a node to the ring ─────────────────────────────────────────────────
  addNode(nodeName, url) {
    const virtualPositions = [];

    for (let i = 0; i < this.virtualNodeCount; i++) {
      let position = this.hash(`${nodeName}:vnode${i}`);

      // Resolve rare collisions by linear probing
      while (this.ring.has(position)) {
        position = (position + 1) % this.hashSpace;
      }

      this.ring.set(position, nodeName);
      virtualPositions.push(position);
      this.sortedPositions.push(position);
    }

    this.sortedPositions.sort((a, b) => a - b);
    this.nodes.set(nodeName, { url, virtualPositions });
  }

  // ─── Remove a node from the ring ────────────────────────────────────────────
  removeNode(nodeName) {
    const nodeData = this.nodes.get(nodeName);
    if (!nodeData) return;

    for (const position of nodeData.virtualPositions) {
      this.ring.delete(position);
      const idx = this.sortedPositions.indexOf(position);
      if (idx !== -1) this.sortedPositions.splice(idx, 1);
    }

    this.nodes.delete(nodeName);
  }

  // ─── Binary search: first position >= keyPosition (clockwise) ───────────────
  _findClockwiseIndex(keyPosition) {
    let left = 0;
    let right = this.sortedPositions.length;

    while (left < right) {
      const mid = Math.floor((left + right) / 2);
      if (this.sortedPositions[mid] < keyPosition) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    // Wrap around to start of ring
    return left >= this.sortedPositions.length ? 0 : left;
  }

  // ─── Get unique physical nodes clockwise from a key position ────────────────
  getNodesForKey(key, replicationFactor = 3) {
    if (this.nodes.size === 0) throw new Error('Hash ring is empty');

    const keyPosition = this.hash(key);
    const startIndex = this._findClockwiseIndex(keyPosition);

    const assignedNodes = [];
    const seenNodes = new Set();
    const total = this.sortedPositions.length;

    for (let i = 0; i < total && assignedNodes.length < replicationFactor; i++) {
      const position = this.sortedPositions[(startIndex + i) % total];
      const nodeName = this.ring.get(position);

      if (!seenNodes.has(nodeName)) {
        seenNodes.add(nodeName);
        assignedNodes.push(nodeName);
      }
    }

    const [primary, ...replicas] = assignedNodes;
    return { primary, replicas, keyHash: keyPosition };
  }

  // ─── Ring visualisation ─────────────────────────────────────────────────────
  getRingInfo() {
    const nodeStats = {};

    for (const [nodeName, data] of this.nodes.entries()) {
      nodeStats[nodeName] = {
        url: data.url,
        virtualNodeCount: data.virtualPositions.length,
        ringCoverage: ((data.virtualPositions.length / this.sortedPositions.length) * 100).toFixed(1) + '%',
      };
    }

    return {
      totalNodes: this.nodes.size,
      virtualNodeCount: this.virtualNodeCount,
      totalVirtualNodes: this.sortedPositions.length,
      hashSpace: `0 → ${this.hashSpace - 1}`,
      nodes: nodeStats,
    };
  }

  getNodeUrl(nodeName) {
    return this.nodes.get(nodeName)?.url;
  }

  getAllNodeNames() {
    return [...this.nodes.keys()];
  }
}

module.exports = HashRing;
