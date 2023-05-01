from kafka import KafkaConsumer
import csv

# Set the Kafka topic to consume from
topic = 'breach.v004'

# Set the location and name of the CSV file to write to
#csv_file_path = '/path/to/csv/file.csv'

# Set up the Kafka consumer
consumer = KafkaConsumer(topic,
                         bootstrap_servers=['10.3.3.1:9092','10.2.3.2:9092','10.1.3.3:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group-id')

# Open the CSV file for writing
with open('/Users/depankarsarkar/Desktop/Breach_kafka/test.csv', mode='w', newline='',encoding='utf-8') as csv_file:
    # Create a CSV writer object
    csv_writer = csv.writer(csv_file)

    # Iterate over the Kafka messages
    for message in consumer:
        # Decode the message value as UTF-8
        message_value = message.value.decode('utf-8')

        # Write the message to the CSV file
        csv_writer.writerow([message.offset, message.timestamp, message_value])
