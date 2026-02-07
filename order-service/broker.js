import amqplib from 'amqplib';

let channel = null;

export async function connectToBroker() {
  try {
    const url = process.env.RABBITMQ_URL || 'amqp://message-broker:5672';
    const conn = await amqplib.connect(url);
    channel = await conn.createChannel();
    console.log('Connected to RabbitMQ');
  } catch (err) {
    console.error('RabbitMQ connection error:', err.message);
    setTimeout(connectToBroker, 5000); // retry after 5s
  }
}

export async function publishMessage(queue, message) {
  if (!channel) {
    console.warn('RabbitMQ channel not ready; dropping message');
    return;
  }
  await channel.assertQueue(queue, { durable: true });
  channel.sendToQueue(queue, Buffer.from(typeof message === 'string' ? message : JSON.stringify(message)), {
    persistent: true
  });
}

export default { connectToBroker, publishMessage };
