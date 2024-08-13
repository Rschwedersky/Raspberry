import time
import concurrent.futures
import threading
import queue
import paho.mqtt.client as mqtt
from picamera2 import Picamera2
from PIL import Image
import piexif
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Initialize sensor data queue
sensor_data_queue = queue.Queue()

# Flag to signal the threads to exit
exit_lock = threading.Lock()

with open('/path/to/secrets.json') as f:
    secrets = json.load(f)

service_account_key_path = secrets['service_account_key_path']
creds = service_account.Credentials.from_service_account_file(service_account_key_path, scopes=['https://www.googleapis.com/auth/drive'])
drive_service = build('drive', 'v3', credentials=creds)
folder_id = secrets['folder_id']

def connect_to_mqtt():
    global exit_flag

    # MQTT broker settings
    mqtt_broker_address = secrets['mqtt_broker_address']
    mqtt_topic = secrets['mqtt_topic']
    mqtt_username = secrets['mqtt_username']
    mqtt_password = secrets['mqtt_password']
    mqtt_port = secrets['mqtt_port']

    # Callback when the client connects to the MQTT broker
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker")
            client.subscribe(mqtt_topic)
        else:
            print("Failed to connect, return code =", rc)

    # Callback when a message is received on the subscribed topic
    def on_message(client, userdata, message):
        global led_state
        global sensor_data2
        sensor_data2 = message.payload.decode('utf-8')
        payload_str = message.payload.decode('utf-8')
        sensor_data_queue.put(payload_str)
        data = json.loads(payload_str)
        led_state = data.get("ledOn")
                

    # Create an MQTT client
    global mqtt_client
    mqtt_client = mqtt.Client("TestClient")

    # Set username and password
    mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)

    # Set TLS/SSL context (since you're using port 8883)
    mqtt_client.tls_set()
    mqtt_client.keep_alive = 60
    # Set callback functions
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    # Connect to the MQTT broker
    mqtt_client.connect(mqtt_broker_address, mqtt_port)

    # Start the MQTT loop (this will handle communication with the broker)
    mqtt_client.loop_forever()
    
def take_photo(picam2):
    try:
        timestamp = time.strftime("%Y%m%d%H%M%S")
        photo_filename = f"{timestamp}.jpeg"
        r = picam2.capture_request()
        r.save("main", photo_filename)
        r.release()
        print("photo")
        return photo_filename
    except Exception as e:
        print(f"Error in take_photo: {str(e)}")

def add_metadata(photo_filename):
    img = Image.open(photo_filename)
    try:
        exif_dict = piexif.load(img.info['exif'])
    except KeyError:
        # If the Exif segment doesn't exist, create a new dictionary
        exif_dict = {'0th': {}, 'Exif': {}, 'GPS': {}, 'Interop': {}, '1st': {}, 'thumbnail': None}

    # Define a custom EXIF tag (replace "42036" with your desired tag ID)
    custom_tag_id = 42036
    # Update the EXIF dictionary with the custom tag and encode the JSON string
    exif_dict["Exif"][custom_tag_id] = sensor_data2
    # Dump the modified EXIF data
    exif_bytes = piexif.dump(exif_dict)
    # Save the modified image with the updated EXIF data
    metadata_image= f"exif_{photo_filename}"
    img.save(metadata_image, "JPEG", exif=exif_bytes)
    # Close the image file
    img.close()
    return metadata_image

def upload_to_google(metadata_image,retry_count=3):
    for attempt in range(retry_count):
        try:
            # Upload the image to the specified folder in Google Drive
            file_metadata = {
                'name': metadata_image,
                'parents': [folder_id],  # Set the folder ID as the parent
            }
            media = MediaFileUpload(metadata_image, mimetype='image/jpeg')
            file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

            # Print the file ID created in Google Drive
            print(f'File ID: {file.get("id")}')
            return True

        except Exception as e:
            print(f"Error during upload attempt {attempt + 1}: {e}")
            time.sleep(10)
    
    # All retries failed, return False
    print(f"Failed to upload {metadata_image} after {retry_count} attempts")
    return False

def control_led(picam2):
    led_topic = "input_led"  # Replace with the actual topic for LED control
    message1 = str(1)
    message2 = str(led_state) 
    try:
        mqtt_client.publish(led_topic, message1, qos=2)
        print(f"Sent LED state: {message1} to topic: {led_topic}")
        time.sleep(10)
        photo_filename=take_photo(picam2)
        time.sleep(10)
        mqtt_client.publish(led_topic, message2, qos=2)
        print(f"Sent LED state: {message2} to topic: {led_topic}")
        return photo_filename
    except Exception as e:
        print(f"Error sending LED control message: {e}")


def capture_and_upload_photo(picam2):
    sensor_data2
    camera_config = picam2.create_still_configuration(main={"size": (1920, 1080)})
    picam2.configure(camera_config)
    picam2.start()
    
    while True:
        start_time=time.time()
        photo_filename=control_led(picam2)
        time.sleep(1)
        try:
            metadata_image=add_metadata(photo_filename)
            ok=upload_to_google(metadata_image)
            if ok:
                os.remove(photo_filename) #TODO os.remove(metadata_image)
                print(f"Picture Uploaded:{metadata_image}")
            else:
                print(f"Picture in Memory{metadata_image}")
                
            end_time=time.time()
            elapsed_time = end_time - start_time
            remaining_time = max(0, 3600 - elapsed_time)
            time.sleep(remaining_time)

        except Exception as e:
            print(f"Error capturing and uploading photo: {str(e)}")

# Start the MQTT thread using ThreadPoolExecutor for concurrent execution
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    picam2 = Picamera2()
    future_mqtt = executor.submit(connect_to_mqtt)

    try:
        # Run the threads indefinitely
        while True:
            future_photo = executor.submit(capture_and_upload_photo,picam2)
            # Your main thread logic here...
            time.sleep(1)  # Adjust as needed

    except KeyboardInterrupt:
        # If you receive a KeyboardInterrupt (e.g., Ctrl+C), set the exit flag
        exit_flag = True

    finally:
        # Wait for both threads to finish
        future_mqtt.result()
        future_photo.result()
