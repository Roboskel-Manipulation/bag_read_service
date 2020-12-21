#!/usr/bin/env python
import sys
import rospy
from std_srvs.srv import Empty, Trigger, EmptyRequest
from keypoint_3d_matching_msgs.msg import Keypoint3d_list


# 3D topic name: /keypoint_3d_matching

def callback(Keypoints_v):
	rospy.wait_for_service('/next_msg')
	try:
		# Create an empty request
		req = EmptyRequest()

		# use the service
		service(req)

	except rospy.ServiceException, e:
		print "Service call failed: %s"%e


if __name__ == "__main__":
	rospy.init_node('clientBag', anonymous=True)

	# Create the connection to the service.
	service = rospy.ServiceProxy('/next_msg', Empty)

	listen_topic = rospy.get_param('~listen_topic') 
	topic_sub = rospy.Subscriber(listen_topic, Keypoint3d_list, callback)
	rospy.spin()
