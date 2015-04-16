# DS_MapReduce
Hadoop MapReduce implementation along with HDFS
There are 2 Master Servers NameNode and Jobtracker.Those can be running on Different Machine.
Datanodes and TaskTrackers must be running on the same machines.
GeneralClient should be used for communication with HDFS hence used in TaskTrackers.

The Config File Fields must be as Follow
************** NameNode Config *********************
threshold=20000
replicationFactor=2
nameNodeIP=10.42.0.62
nameNodePort=20000
blockSize=16777216

************** DataNode Config *********************
dataNodeID=10
dataNodeIP=10.42.0.62
dataNodePort=50000
nameNodeIP=10.42.0.62
nameNodePort=20000
heartBeatRate=5000
directoryName=/home/shrikant/blockDirectory/

************** GeneralClient Config *********************
This is the Client which communicates with HDFS for Coping files to HDFS (write method) and Coping or reading files from HDFS(read method)
nameNodeIP=10.42.0.62
nameNodePort=20000
blockSize=16777216

************** JobTracker Config *********************
JobTracker should have details of all the taskTrackers i.e. TaskTackerId,TaskTrackerIp,TaskTrackerPort.
This is the Server which will handle the execution of MapReduce task.
NameNodeIp=10.42.0.62
NameNodePort=20000
TaskTrackerId=10
TaskTrackerIp=10.42.0.62
TaskTrackerPort=50000
TaskTrackerId=20
TaskTrackerIp=10.42.0.1
TaskTrackerPort=60000
JobTrackerIp=10.42.0.1
JobTrackerPort=40000

************** TaskTracker Config *********************
taskTrackerId=10
noOfMapThreads=25
noOfReduceThreads=2
JobTrackerIp=10.42.0.1
JobTrackerPort=40000
heartBeatRate=10000
genClientConf=/home/shrikant/git/DS_MapReduce/Config/GeneralClientConf
tmpMapDir=/home/shrikant/blockDirectory/MapTmp/
tmpReduceDir=/home/shrikant/blockDirectory/ReduceTmp/
blockDir=/home/shrikant/blockDirectory/
jarPath=/home/shrikant/git/DS_MapReduce/DS_MapReduce/bin/MapReduce.jar
searchKey=test

************** JobClient Config *********************
This is a client which will submit the task to Jobtracker.

jobTrackerIP=10.42.0.1
jobTrackerPort=40000
jobStatusResponseTime=3000
genClientConfFile=/home/shrikant/git/DS_MapReduce/Config/GeneralClientConf
