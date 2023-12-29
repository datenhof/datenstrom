from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:9093',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['datenstrom_raw'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value()))

c.close()