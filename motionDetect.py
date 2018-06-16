# import the necessary packages
import argparse
import datetime
import imutils
import time
import cv2

import numpy as np
from kafka import KafkaConsumer

"""
This consumes a kafka topic and posts the image to a browser

It just wraps the image"""

from flask import Flask, Response

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
	i = 0
	global firstFrame
	print("in and running")
	for msg in consumer:

		# print the message number and the length of the mesage
		print("v == {} : {}".format(i, len(msg.value))
		)
		i += 1
		# nparr = np.fromstring(msg.value, np.uint8)
		nparr = np.frombuffer(msg.value, dtype='uint8')
		img_np = cv2.imdecode( nparr, cv2.IMREAD_COLOR)
		# print(type(img_np)) # should be an np.nparray
		# nparr = np.fromstring(msg.value, np.uint8)
		# img_np = cv2.imdecode(nparr, cv2.COLOR_BGR2GRAY) # cv2.IMREAD_COLOR in OpenCV 3.1
		#
		# # resize the frame, convert it to grayscale, and blur it
		# # frame = imutils.resize(frame, width=500)
		gray = cv2.cvtColor(img_np, cv2.COLOR_BGR2GRAY)
		gray = cv2.GaussianBlur(gray, (21, 21), 0)

		# if the first frame is None, initialize it
		if firstFrame is None:
			firstFrame = gray
			continue
		# print(type(firstFrame.shape))
		# ============================================================
		# compute the absolute difference between the current frame and
		# first frame
		frameDelta = cv2.absdiff(firstFrame, gray)
		# drop pixels < 25 and paint pixels > 25, white
		ret, thresh = cv2.threshold(frameDelta, 25, 255, cv2.THRESH_BINARY)
		print("here 1")
		# dilate the thresholded image to fill in holes, then find contours
		# on thresholded image
		thresh = cv2.dilate(thresh, None, iterations=2)
		(_,cnts, _) = cv2.findContours(thresh.copy(), cv2.RETR_TREE,
			cv2.CHAIN_APPROX_SIMPLE)
		print("here 2")
		# loop over the contours
		for c in cnts:
			# if the contour is too small, ignore it
			if cv2.contourArea(c) < 500:
				continue

			# compute the bounding box for the contour, draw it on the frame,
			# and update the text
			(x, y, w, h) = cv2.boundingRect(c)
			cv2.rectangle(img_np, (x, y), (x + w, y + h), (0, 255, 0), 2)
			text = "Occupied"
			# draw the text and timestamp on the frame
			cv2.putText(img_np, "Room Status: {}".format(text), (10, 20),
				cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2)
			cv2.putText(img_np, datetime.datetime.now().strftime("%A %d %B %Y %I:%M:%S%p"),
				(10, img_np.shape[0] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.35, (0, 0, 255), 1)
		f = cv2.imencode('jpg', img_np)
		yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + f.toBytes() + b'\r\n\r\n')
			# # show the frame and record if the user presses a key
			# cv2.imshow("Security Feed", img_np)
			# cv2.imshow("Thresh", thresh)
			# cv2.imshow("Frame Delta", frameDelta)
			# key = cv2.waitKey(1) & 0xFF
			#
			# # if the `q` key is pressed, break from the lop
			# if key == ord("q"):
			# 	break



if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
