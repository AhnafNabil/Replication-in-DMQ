const axios = require('axios');

const COORDINATOR_URL = process.env.COORDINATOR_URL || 'http://localhost:7000';

// Keys to consume — passed as CLI args or use default list
const keysToConsume = process.argv.slice(2).length > 0
  ? process.argv.slice(2)
  : ['order_1', 'order_2', 'order_3', 'payment_1', 'payment_2', 'user_signup_5', 'user_signup_12', 'inventory_101'];

function printSeparator() {
  console.log('─'.repeat(70));
}

async function consume() {
  console.log('\n' + '='.repeat(70));
  console.log('  CONSUMER — Reading messages from Distributed Message Queue');
  console.log('='.repeat(70));
  console.log(`  Coordinator : ${COORDINATOR_URL}`);
  console.log(`  Keys        : ${keysToConsume.join(', ')}`);
  console.log('='.repeat(70) + '\n');

  // Show current failover state
  try {
    const failoverRes = await axios.get(`${COORDINATOR_URL}/failover/status`);
    const { totalFailovers, activePromotions } = failoverRes.data;
    if (totalFailovers > 0) {
      console.log(`⚠️  FAILOVER ACTIVE: ${totalFailovers} failover(s) have occurred`);
      for (const [failed, promoted] of Object.entries(activePromotions)) {
        console.log(`   ${failed} → replaced by ${promoted}`);
      }
      console.log();
    }
  } catch {
    // coordinator may not have failover endpoint if it just started
  }

  console.log('📥 Consuming messages:\n');
  printSeparator();

  for (const key of keysToConsume) {
    try {
      const res = await axios.get(`${COORDINATOR_URL}/consume/${key}`);
      const { payload, servedBy, source, failover } = res.data;

      console.log(`Key      : ${key}`);
      console.log(`Payload  : ${JSON.stringify(payload)}`);
      console.log(`Served By: ${servedBy}  (source: ${source})`);
      if (failover) {
        console.log(`⚡ FAILOVER: Data retrieved from replica after primary failure`);
      }
    } catch (err) {
      const reason = err.response?.data?.error || err.message;
      console.log(`Key      : ${key}`);
      console.log(`ERROR    : ${reason}`);
    }
    printSeparator();
    await new Promise(r => setTimeout(r, 200));
  }

  console.log('\n✅ Done consuming.\n');
}

consume().catch(err => {
  console.error('Consumer error:', err.message);
  process.exit(1);
});
