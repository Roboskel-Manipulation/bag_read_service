#!/usr/bin/env python

"""
Server providing the service of reading a rosbag file on demand.
On a call of /next_msg either by a client or just "rosservice call /next_msg" in a terminal
the Server reads the next message for each topic in the rosbag file and publishes them on their topics
"""

from sensor_msgs.msg import Image, PointCloud2, CameraInfo
import roslib, rospy, rosbag
from std_srvs.srv import Empty,EmptyResponse, Trigger
import sys
from rosgraph_msgs.msg import Clock, Log
from tf2_msgs.msg import TFMessage
from keypoint_3d_matching_msgs.msg import Keypoint3d_list, Keypoint3d

__author__ = "Lygerakis Fotios"
__license__ = "GPL"
__email__ = "ligerfotis@gmail.com"

class ReadRosBagService():

    def __init__(self, bagfile):
        self.bag = rosbag.Bag(bagfile, 'r')

        self.publishers = {}
        self.messages = {}
        self.listOfCounts = {}
        self.listOfEndConditions = {}
        
        #Create a publisher for each topic
        self.topics, self.msg_types = self.get_topic_and_type_list()

        for topic in self.topics:

            # create list of publishers for each topic
            self.publishers[topic] = rospy.Publisher(topic, self.msg_types[topic], queue_size = 10)

            # create a list of messages for each topic
            self.messages[topic] = self.bag.read_messages(topics= topic)

            # a counter of msgs for each topic
            self.listOfCounts[topic] = 0

            # flag when generator emptied for each topic
            self.listOfEndConditions[topic] = False
        # publisher for clock
        self.clockPublisher = rospy.Publisher("/clock", Clock, queue_size = 10)
        # create a Server
        self.next_msgs_srv = rospy.Service('/next_msg', Empty, self.service)

    def get_topic_and_type_list(self):
        # retrieve a list of topics from the rosbag file
        info = self.bag.get_type_and_topic_info()
        topics = list(info[1].keys())
        
        # retrieve a list of message types from the rosbag file
        # NOT WORKING PROPERLY | USE THE HARD-CODED LIST
        types = {}
        for topic in topics:
            types[topic] = eval((info[1][topic][0]).split("/")[1])
        
        return topics, types 

    def pub_next_msg(self):
        time = None
        for topic in self.topics:
            try:
                _, msg, t = self.messages[topic].next()
            except:
                self.listOfEndConditions[topic] = True
                continue
            if time is None:
                time = t
            self.publishers[topic].publish(msg)
            self.listOfCounts[topic] += 1
        self.clockPublisher.publish(time)

    def service(self, req):
        try:
            self.pub_next_msg()
            return EmptyResponse()
        except RuntimeError,e:
            rospy.logerr("Exception caught:\n%s", e)
            return 0 

if __name__=='__main__':

    rospy.init_node('bag_by_service')
    
    # Rosbag file 
    bag_file = sys.argv[1]
    
    #Run this file with the name of the bag file
    rrbs = ReadRosBagService(bag_file)

    while not rospy.is_shutdown():
        if any(value for value in rrbs.listOfEndConditions.values()):
            break
        # rospy.spin()

    for topic in rrbs.topics:
        print("Bag Reader: Total published msgs from topic %s: %d "%(topic, rrbs.listOfCounts[topic]))
    
    # Wait a bit before closing the rosbag
    rospy.sleep(0.5)
    rrbs.bag.close()
