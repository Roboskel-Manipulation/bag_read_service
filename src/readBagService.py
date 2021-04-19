#!/usr/bin/env python

"""
Server providing the service of reading a rosbag file on demand.
On a call of /next_msg either by a client or just "rosservice call /next_msg" in a terminal
the Server reads the next message for each topic in the rosbag file and publishes them on their topics
"""

from sensor_msgs.msg import Image, PointCloud2, CameraInfo
import roslib, rospy, rosbag
from std_srvs.srv import Empty, EmptyRequest, EmptyResponse, Trigger
import sys, time
from rosgraph_msgs.msg import Clock, Log
from tf2_msgs.msg import TFMessage
from std_msgs.msg import Bool
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
        self.count = 0

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

        # sometimes, more than one service call was needed for the pipeline to start
        # the count "7" was experimentally chosen
        while self.count < 7:
            rospy.loginfo("Gonna sleep for 3 secs")
            time.sleep(3)
            rospy.loginfo("Woke up")
            service = rospy.ServiceProxy('/next_msg', Empty)
            rospy.wait_for_service('/next_msg')
            try:
                # Create an empty request
                req = EmptyRequest()
                # use the service
                service(req)
            except rospy.ServiceException, e:
                print "Service call failed: %s"%e
            self.count += 1


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

    end_rosbag_read_pub = rospy.Publisher("/end_rosbag_read", Bool, queue_size=10)
   
    #Run this file with the name of the bag file
    rrbs = ReadRosBagService(bag_file)

    while not rospy.is_shutdown():
        if any(value for value in rrbs.listOfEndConditions.values()):
            break
        # rospy.spin()

    for topic in rrbs.topics:
        print("Bag Reader: Total published msgs from topic %s: %d "%(topic, rrbs.listOfCounts[topic]))
    
    # Wait a bit before closing the rosbag
    time.sleep(0.5)
    rospy.loginfo("Gonna close input rosbag")
    rrbs.bag.close()
    end_msg = Bool()
    end_msg.data = True
    end_rosbag_read_pub.publish(end_msg)
