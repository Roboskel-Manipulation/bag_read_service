#!/usr/bin/env python
from keypoint_3d_matching_msgs.msg import Keypoint3d_list
import rospy
import tf
import geometry_msgs 
from rosgraph_msgs.msg import Clock
from keypoint_3d_matching_msgs.msg import Keypoint3d_list
import sys
import rosbag


class TFBagReader():

    def __init__(self, bagfile):
        self.bag = rosbag.Bag(bagfile, 'r')  

        self.messages = {}

        self.topics = ["/keypoints_3d_matching", "/tf"]
        for topic in self.topics:
            # create a list of messages for each topic
            self.messages[topic] = self.bag.read_messages(topics= topic)

         # publisher for keypoints_3d_matching
        self.keypointPublisher = rospy.Publisher("/keypoints_3d_matching", Keypoint3d_list, queue_size = 10)
        # publisher for clock
        self.clockPublisher = rospy.Publisher("/clock", Clock, queue_size = 10)

        self.listener = tf.TransformListener()

        self.counter = 0

    def get_tf(self):
        while(1):
            try:
                (trans, rot) = self.listener.lookupTransform("/base_link", "/camera_rgb_optical_frame", rospy.Time.now())
                return trans
            except (tf.LookupException, tf.ConnectivityException, tf.ExtrapolationException):
                print("here")
                _, msg, _ = self.messages["/tf"].next()

            #while self.listener.canTransform("/base_link", "/camera_rgb_optical_frame", rospy.Time(1)):
        return None


    def pub_next_msg(self):
        _, msg, t = self.messages["/Keypoint3d_list"].next()

        self.clockPublisher.publish(t)
        self.Keypoint3d_list.publish(msg)
        self.counter += 1

    def transform_keypoints(self):
        trans = self.get_tf()
        print(trans)
        input()
        # print(tf_msg)
                
        # try:
        # #     now = rospy.Time.now()
        # #     future = now + rospy.Duration(20.0)
        # #     listener.waitForTransform("/base_link", now, "/turtle1", future, "/camera_rgb_optical_frame", rospy.Duration(1.0))
        #     (trans, rot) = listener.lookupTransform("/base_link", "/camera_rgb_optical_frame", rospy.Time(1))
        # except (tf.LookupException, tf.ConnectivityException, tf.ExtrapolationException):
        #     print("Exception")
        # print trans


if __name__=='__main__':

    rospy.init_node('bag_by_service')

    #run this file with the name of the bag file
    tfb = TFBagReader(sys.argv[1])
    tfb.transform_keypoints()
    # while not rospy.is_shutdown():
    #     if all(value for value in tfb.listOfEndConditions.values()):
    #         break
    #     rospy.spin()

    print("Bag Reader: Total published msgs from topic /keypoints_3d_matching: %d "%(tfb.counter))
    
    tfb.bag.close()