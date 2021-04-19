#!/usr/bin/env python
import sys
import rospy, rosbag, time
from std_srvs.srv import Empty, Trigger, EmptyRequest
from openpose_ros_msgs.msg import OpenPoseHumanList
from keypoint_3d_matching_msgs.msg import Keypoint3d_list
from std_msgs.msg import Bool

# 3D topic name: /keypoint_3d_matching

def openpose_callback(msg):
	global bag_write_openpose
	bag_write_openpose.write("/openpose_ros/human_list", msg)

def keypoints3d_callback(msg):
	global bag_write_3D
	bag_write_3D.write("/transform_topic", msg)

def callback(msg):
	global count, listen_topic
	count += 1
	rospy.wait_for_service('/next_msg')
	try:
		# Create an empty request
		req = EmptyRequest()

		# use the service
		service(req)

	except rospy.ServiceException, e:
		print "Service call failed: %s"%e

def end_rosbag_read_callback(msg):
	global end_flag
	end_flag = True

if __name__ == "__main__":
	end_flag = False
	count = 0
	rospy.init_node('clientBag')
	fileName = sys.argv[1].split('/')[-1].split('.')[0]

	bag_write_3D = rosbag.Bag('/home/fdeli/test_autonomous_bag_read_server/rosbags/3D_new/CXF/{}.bag'.format(fileName), 'w')
	bag_write_openpose = rosbag.Bag('/home/fdeli/test_autonomous_bag_read_server/rosbags/2D/CXF/{}.bag'.format(fileName), 'w')

	bag = rosbag.Bag(sys.argv[1], 'r')
	info = bag.get_type_and_topic_info()
	topics = list(info[1].keys())
	messages = {}
	for topic in topics:
		messages[topic] = bag.read_messages(topics=topic)
	max_length = min([sum([1 for _ in messages[i]]) for i in messages.keys()])

	# Create the connection to the service.
	service = rospy.ServiceProxy('/next_msg', Empty)

	listen_topic = rospy.get_param('~listen_topic') 
	
	# subscriber to keypoint_3d_matching (3D keypoints expressed in the camera frame)
	topic_sub = rospy.Subscriber(listen_topic, Keypoint3d_list, callback)
	
	# subscriber to openpose_ros/human list (openpose keypoints)
	openpose_sub = rospy.Subscriber("/openpose_ros/human_list", OpenPoseHumanList, openpose_callback)

	# subscriber to transform_topic (3D keypoints expressed in the base_link frame)
	keypoints3d_sub = rospy.Subscriber("/transform_topic", Keypoint3d_list, keypoints3d_callback)

	# subscriber to the end_rosbag_read (topic is published when the rosbag reader reads the entire rosbag)
	end_rosbag_read_sub = rospy.Subscriber("/end_rosbag_read", Bool, end_rosbag_read_callback)

	while True:
		print(count, max_length)
		if end_flag:
			break

	rospy.loginfo("Gonna sleep for 2 secs")
	time.sleep(2)
	rospy.loginfo("Woke up....gonna exit")
	bag.close()
	bag_write_3D.close()
	bag_write_openpose.close()
