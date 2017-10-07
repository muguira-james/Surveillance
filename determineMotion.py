
"""
This consumes a kafka topic and posts the image to a browser

Process the image to determine if there is movement
"""
import numpy as np
import cv2
from flask import Flask, Response
from kafka import KafkaConsumer
import argparse
import datetime
import imutils
import time

firstFrame = None
#connect to Kafka server and pass the topic we want to consume
consumer = KafkaConsumer(
    'my-topic_txt',

    group_id='view',
    bootstrap_servers=['0.0.0.0:9092'])
#Continuously listen to the connection and print messages as recieved
app = Flask(__name__)

@app.route('/')
def index():
    # return a multipart response
    # print("consuming something")
    return Response(kafkastream(),
                    mimetype='multipart/x-mixed-replace; boundary=frame')
def kafkastream():
    # firstFrame is the source frame for computing the diff against
    #
    # algorithm
    # grab a frame from kafka.  The frame is joeg encoded. Convert it into
    # no.nparray and finally into the open cv format bgr
    #
    # do a blur, a diff against the firstframe, threshold and find contours
    #
    # loop through the contours: markup the image with time date and an indicator is
    # we see movement
    #
    global firstFrame
    for msg in consumer:

        # print("{}:".format(type(msg.value)))
        nparr = np.frombuffer(msg.value, dtype='uint8')
        img_np = cv2.imdecode( nparr, cv2.IMREAD_COLOR)
        gray = cv2.cvtColor(img_np, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (21, 21), 0)

        # if the first frame is None, initialize it
        if firstFrame is None:
            firstFrame = gray
            continue

    	# compute the absolute difference between the current frame and
    	# first frame
        frameDelta = cv2.absdiff(firstFrame, gray)
        thresh = cv2.threshold(frameDelta, 25, 255, cv2.THRESH_BINARY)[1]

    	# dilate the thresholded image to fill in holes, then find contours
    	# on thresholded image
        thresh = cv2.dilate(thresh, None, iterations=2)
        (_, cnts, _) = cv2.findContours(thresh.copy(), cv2.RETR_EXTERNAL,
	       cv2.CHAIN_APPROX_SIMPLE)

        img2Show = img_np
    	# loop over the contours

        for c in cnts:
            # if the contour is too small, ignore it
            area = cv2.contourArea(c)

            if area < 5000:
                continue

            print("{}".format(area))
    		# compute the bounding box for the contour, draw it on the frame,
    	    # and update the text
            (x, y, w, h) = cv2.boundingRect(c)
            cv2.rectangle(img2Show, (x, y), (x + w, y + h), (0, 255, 0), 2)
            text = "Occupied"

        	# draw the text and timestamp on the frame
            cv2.putText(img2Show, "Room Status: {}".format(text), (10, 20),
                cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2)
            # Print the frame time
            cv2.putText(img2Show, datetime.datetime.now().strftime("%A %d %B %Y %I:%M:%S%p"),
                (10, img2Show.shape[0] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.75, (0, 0, 0), 1)

        # clear the contours list and ready the image for display in the browser
        del cnts[:]
        ret, jpeg = cv2.imencode('out.jpg', img2Show)
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + jpeg.tobytes() + b'\r\n\r\n')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
