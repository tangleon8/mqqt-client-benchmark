import paho.mqtt.client as mqtt
import time

my_id = "benchmark-1"
expected_benchmarks = ["benchmark-1", "benchmark-2", "benchmark-3", "benchmark-4"]
broker_address = "localhost"
broker_port = 1883
test_topics = ["test/topic1", "test/topic2"]

readiness = {bench: False for bench in expected_benchmarks}

client = mqtt.Client(client_id=my_id)
client.connect(broker_address, broker_port)

def on_message(client, userdata, message):
    topic = message.topic
    if topic.startswith("benchmark/ready_state/"):
        bench_id = topic.split("/")[-1]
        if bench_id in readiness:
            readiness[bench_id] = True
            print(f"Received readiness from {bench_id}")
            if all(readiness.values()):
                start_test()
    elif topic in test_topics:
        print(f"Received test message on {topic}: {message.payload.decode()}")

client.on_message = on_message

for topic in test_topics:
    client.subscribe(topic)
    print(f"Subscribed to test topic {topic}")

for bench in expected_benchmarks:
    topic = f"benchmark/ready_state/{bench}"
    client.subscribe(topic)
    print(f"Subscribed to {topic}")

own_topic = f"benchmark/ready_state/{my_id}"
client.publish(own_topic, "ready", retain=True)
print(f"Published readiness to {own_topic}")

client.loop_start()

def start_test():
    print("All benchmarks are ready. Starting test...")
    for i in range(10):
        message = f"Message {i} from {my_id}"
        client.publish("test/topic1", message)
        print(f"Published: {message}")
        time.sleep(0.1)
    time.sleep(2)
    client.loop_stop()
    client.disconnect()
    print("Test completed.")

try:
    while client.is_connected():
        time.sleep(1)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
    print("Disconnected due to interrupt.")
