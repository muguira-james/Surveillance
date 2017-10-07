# producer.py
"""
This samples the camera and places every image on a kafka topic

I reduce the image size to 300,200 because kafka has a 1,000,000 bytes
size limit.  May experiment with increasing that
"""

import time
import cv2
from kafka import SimpleProducer, KafkaClient
import base64

#  connect to Kafka
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)
# Assign a topic
topic = 'my-topic_txt'

"""
This is a test function: it just writes a text message to the topic
"""
def emitter():
    for j in range(1,500):
        txt = b'Hello James'
        print("try to send our message: {}".format(txt))
        producer.send_messages(topic, txt)
        time.sleep(0.1)

"""
This is the real worker.  It reads the camera, scales the image to 300,200
and places it into a kafka topic
"""
def video_emitter(video):
    # Open the video
    video = cv2.VideoCapture(0)
    #video = cv2.VideoCapture(video)
    print(' emitting.....')
    i = 0
    # read the file
    while (video.isOpened):
        # read the image in each frame
        success, image = video.read()
        # check if the file has read to the end
        if not success:
            break
        i += 1
        if i > 1000:
            print("shape: {}: type: {}".format(image.shape, type(image)))
            i = 0
        cv2.resize(image, (300,200), interpolation = cv2.INTER_CUBIC);
        # convert the image to jpg
        ret, jpeg = cv2.imencode('out.jpg', image)
        # Convert the image to bytes and send to kafka
        producer.send_messages(topic, jpeg.tobytes())
        # To reduce CPU usage create sleep time of 0.2sec
        time.sleep(0.2)
    # clear the capture
    video.release()
    print('done emitting')

if __name__ == '__main__':
    video_emitter('video.mp4')
    # emitter()
