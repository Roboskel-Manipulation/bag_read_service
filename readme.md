# bag_read_service

### Description

##### Rosbag Read Server

###### Service Provided
- Reads from a rosbag one message at a time for every topic.
- Publishes each message at its topic.

##### Rosbag Read Client

###### Functionality
- Listens to a specific topic.
- When a message from this topic is received, asks a RR service.

__Note:__ Define the topic name in the bag.launch and the message type in the clientBag.py.

##### Server-Client Loop
- Call the service once to start the loop.
- When the client listens to its specified topic, it makes the next service call.
- The loop stops when there is no other message.


### Run Code

In a __new terminal__ run
        
        roslaunch bag_read_service bag_read_service.launch bag_file:="filename"

In a __new terminal__ run 

        rosservice call /next_msg

to read the first message from the rosbag file and start the pipeline.
