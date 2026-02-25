const axios = require('axios');

const COORDINATOR_URL = process.env.COORDINATOR_URL || 'http://localhost:7000';

// ─── Messages to produce ─────────────────────────────────────────────────────
const messages = [
  { key: 'order_1',        payload: { event: 'order_placed',   userId: 42,  amount: 199.99, item: 'Laptop' } },
  { key: 'order_2',        payload: { event: 'order_placed',   userId: 17,  amount: 49.99,  item: 'Mouse' } },
  { key: 'order_3',        payload: { event: 'order_placed',   userId: 88,  amount: 299.00, item: 'Monitor' } },
  { key: 'payment_1',      payload: { event: 'payment_done',   userId: 42,  amount: 199.99, status: 'confirmed' } },
  { key: 'payment_2',      payload: { event: 'payment_done',   userId: 17,  amount: 49.99,  status: 'confirmed' } },
  { key: 'user_signup_5',  payload: { event: 'user_registered', userId: 5,  email: 'alice@example.com' } },
  { key: 'user_signup_12', payload: { event: 'user_registered', userId: 12, email: 'bob@example.com' } },
  { key: 'inventory_101',  payload: { event: 'stock_updated',  productId: 101, stock: 50 } },
];

function printSeparator() {
  console.log('─'.repeat(70));
}

async function produce() {
  console.log('\n' + '='.repeat(70));
  console.log('  PRODUCER — Sending messages to Distributed Message Queue');
  console.log('='.repeat(70));
  console.log(`  Coordinator : ${COORDINATOR_URL}`);
  console.log(`  Messages    : ${messages.length}`);
  console.log('='.repeat(70) + '\n');

  // First, show the ring layout
  try {
    const ringRes = await axios.get(`${COORDINATOR_URL}/ring`);
    console.log('📍 Hash Ring State:');
    const { nodes } = ringRes.data;
    for (const [name, info] of Object.entries(nodes)) {
      console.log(`   ${name.padEnd(10)} → ${info.url.padEnd(30)} coverage: ${info.ringCoverage}`);
    }
    console.log();
  } catch (err) {
    console.error('Could not reach coordinator. Is Docker Compose running?');
    process.exit(1);
  }

  console.log('📨 Producing messages:\n');
  printSeparator();

  const results = [];

  for (const msg of messages) {
    try {
      const res = await axios.post(`${COORDINATOR_URL}/produce`, {
        key: msg.key,
        payload: msg.payload,
      });

      const { primary, replicas, keyHash, replicationResults } = res.data;
      const replicaStatus = replicationResults
        .map(r => `${r.node.split('//')[1]?.split(':')[0] || r.node} (${r.status})`)
        .join(', ');

      console.log(`Key     : ${msg.key}`);
      console.log(`Hash    : ${keyHash}`);
      console.log(`Primary : ${primary}`);
      console.log(`Replicas: ${replicas.join(', ')}`);
      console.log(`RepStatus: ${replicaStatus}`);
      printSeparator();

      results.push({ key: msg.key, primary, replicas, success: true });
    } catch (err) {
      console.log(`Key     : ${msg.key}`);
      console.log(`ERROR   : ${err.response?.data?.error || err.message}`);
      printSeparator();
      results.push({ key: msg.key, success: false });
    }

    // Small delay so logs are readable
    await new Promise(r => setTimeout(r, 300));
  }

  console.log('\n✅ All messages produced.\n');
  console.log('Summary:');
  for (const r of results) {
    const status = r.success
      ? `Primary=${r.primary}  Replicas=[${r.replicas?.join(', ')}]`
      : 'FAILED';
    console.log(`  ${r.key.padEnd(20)} → ${status}`);
  }
  console.log();
}

produce().catch(err => {
  console.error('Producer error:', err.message);
  process.exit(1);
});
