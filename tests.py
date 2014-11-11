from pika import BlockingConnection, ConnectionParameters, PlainCredentials, BasicProperties

m2m_conn = BlockingConnection(
    ConnectionParameters(
        host="192.168.33.10", 
        port=5672))
channel = m2m_conn.channel()
channel.queue_declare(queue="storm", durable=True)
for i in range(20):
    channel.basic_publish(
        exchange='',
        routing_key="storm", 
        body="message %d" % i,
        properties=BasicProperties(delivery_mode=2))
