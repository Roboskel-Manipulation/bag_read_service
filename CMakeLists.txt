cmake_minimum_required(VERSION 2.8.3)
project(bag_read_service)

find_package(catkin REQUIRED COMPONENTS
  keypoint_3d_matching_msgs
)

catkin_package()

# install the launch files
install(DIRECTORY launch/
        DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch)
