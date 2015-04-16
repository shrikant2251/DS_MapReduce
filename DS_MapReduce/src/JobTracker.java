import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import HDFSPackage.INameNode;
import HDFSPackage.IpConversion;
import HDFSPackage.RequestResponse.*;
import MapReducePkg.MRRequestResponse.HeartBeatResponse;
import MapReducePkg.MRRequestResponse.*;

public class JobTracker extends UnicastRemoteObject implements IJobTracker{


	protected JobTracker(String confFile) throws RemoteException {
		super();
		conf(confFile);
		// TODO Auto-generated constructor stub
	}

	public static String nameNodeIP = "127.0.0.1";
	public static int nameNodePort=1088;//blockSize=10;
	/****
	 * Method to open File from HDFS in read or write Mode
	 */
	/* openFile will return the response of openFileResponse*/
	public byte[] open(String fileName, boolean forRead) {
		byte[] response = new byte[1];
		int status = 1;
		try {
			Registry myreg = LocateRegistry.getRegistry(nameNodeIP,nameNodePort);
			INameNode in = (INameNode) myreg.lookup("NameNode");
			OpenFileRequest openFileRequest = new OpenFileRequest(fileName,forRead);
			response = in.openFile(openFileRequest.toProto());
			//System.out.println("In JobTracker Open method Connection to NameNode is successful OpenFile success");
		} catch (Exception e) {
			status = -1;
			e.printStackTrace();
		}
		if (status == -1) {
			//System.out.println("In JobTracker Open method OpenFile Failed");
			OpenFileRespose openFileResponse = new OpenFileRespose(-1, -1, new ArrayList<Integer>());
			response = openFileResponse.toProto();
		}

		return response;
	}
	/***
	 * Method to get Block Locations from NameNode
	 * @return
	 */
	public ArrayList<BlockLocations> getBlockLocations(ArrayList<Integer> blockNumbers){
		
		INameNode in = null;
		//IDataNode dataNode = null;
		//IpConversion ipObj = new IpConversion();
	//	int RMIStatus = 1;
		for(int blk : blockNumbers){
			//System.out.println("JobTracker getBlockLocation :" + blk);
		}
		try {
			Registry myreg = LocateRegistry.getRegistry(nameNodeIP,nameNodePort);
			in = (INameNode) myreg.lookup("NameNode");
		} catch (Exception e) {
			//RMIStatus = -1;
			//System.out.println("GeneralClient :: RMI Error locating NameNode Registry.");
			e.printStackTrace();
			//System.out.println("JobTracker Method getBlockLocations Failure due to RMI call to NameNode failed");
			return null;
		}
		
		// get locations for list of blocks returned by openFile
		BlockLocationRequest blockLocationRequest = new BlockLocationRequest(blockNumbers);
		byte[] getBlockLocationResponse = null;
		try {
			byte [] temp = blockLocationRequest.toProto();
			getBlockLocationResponse = in.getBlockLocations(temp);
		} 
		catch (Exception e1) {
			//System.out.println("GeneralClient :: Error retrieving block locations.");
			e1.printStackTrace();
			//return status;
			//System.out.println("JobTracker Method getBlockLocations Failure due to getBlockLocations failed");
			return null;
		}
		
		BlockLocationResponse blockLocationResponse = new BlockLocationResponse(getBlockLocationResponse);
		if (blockLocationResponse.status == -1) {
			//System.out.println("JobTracker Method getBlockLocations Failure due to BlockLocationResponse failed");
			return null;
		}
		//System.out.println("JobTracker getBlockLocation Method number of Blocks : " + blockLocationResponse.blockLocations.size());
		return blockLocationResponse.blockLocations;
	}
	@Override
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	public byte[] jobSubmit(byte[] jobSubmitRequest) {
		JobSubmitResponse jsResponse = new JobSubmitResponse();
		JobSubmitRequest jsRequest = new JobSubmitRequest(jobSubmitRequest);
		
		// opens the file in HDFS in Read Mode
		byte[] openResponse;
		//System.out.println("Sending Request to NameNode for Open File " + jsRequest.inputFile );
		openResponse = open(jsRequest.inputFile, true);
		OpenFileRespose openFileResponse = new OpenFileRespose(openResponse);

		// check status of openFile
		if (openFileResponse.status == -1) {
			// openFile Unsuccessful
			jsResponse.status = -1;
			jsResponse.jobId = -1;
			System.out.println("Jobtracker jobSubmit method OpenFileResponse Failed ");
			return jsResponse.toProto();
		}
		//  obtain the block locations to determine the number of map tasks required.
		//	BlockLocationRequest blockLocationRequest = new BlockLocationRequest(openFileResponse.blockNums);
		ArrayList<BlockLocations> blockLocations = getBlockLocations(openFileResponse.blockNums);
		jsResponse.status = 1;
		jsResponse.jobId = DSForJT.jobId++;
		//TODO Keep HashMap of JobId to TaskTrackers or blockLocations used later in getJobStatus
		//DSForJT.jobIdtoTaskTracker.put(jsResponse.jobId, blockLocations);
		JobResponseData tempRed = new JobResponseData();
		tempRed.reduceName = jsRequest.reduceName;
		double d1 = blockLocations.size();
		double d2 = jsRequest.numReduceTasks;
		tempRed.mapFileForEachReducer = (int)(d1/ d2);
		tempRed.totalMap = blockLocations.size();
		tempRed.totalReduce = jsRequest.numReduceTasks;
		//System.out.println("Jobtracker Jobsubmit Map Files for each Reducer :" + tempRed.mapFileForEachReducer);
		//System.out.println("Jobtracker Jobsubmit total Map : "  + tempRed.totalMap +" Total reduce Tasks:" + tempRed.totalReduce);
		tempRed.mapStarted = 0;
		tempRed.reduceStarted = 0;
		DSForJT.jobIdtoJobresponse.put(jsResponse.jobId, tempRed);
		for(BlockLocations block : blockLocations){
			//System.out.println("Jobtracker jobSumit Method for loop block num : " + block.blockNumber);
			MapTaskInfo tempMapTask = new MapTaskInfo();
			tempMapTask.jobId = jsResponse.jobId;
			tempMapTask.taskId = DSForJT.taskId++;
			tempMapTask.mapName = jsRequest.mapName;
			tempMapTask.inputBlocks = block.blockNumber;
			//System.out.println("JobTracker jobSubmit method DataNodeLocation list size :" + block.locations.size());
		/*	for(DataNodeLocation dls : block.locations){
				System.out.println("JobTracker JobSubmit Method for loop DataNodeLocation ip:" + dls.ip + "  port:" + dls.port );
			}
			*/
			for(DataNodeLocation dls : block.locations){
				int TTid = DSForJT.TTLocToTTid.get(dls);
		//		System.out.println("JobTracker JobSumit ForLoop for DataNodeLocation " + TTid);
				if(DSForJT.TTtoJobs.containsKey(TTid))
					DSForJT.TTtoJobs.get(TTid).add(tempMapTask);
				else{
					ArrayList<MapTaskInfo> tempJobList = new ArrayList<MapTaskInfo>();
					tempJobList.add(tempMapTask);
					DSForJT.TTtoJobs.put(TTid, tempJobList);
				}
				if(DSForJT.jobstoTT.containsKey(tempMapTask)){
					DSForJT.jobstoTT.get(tempMapTask).add(TTid);
				}
				else{
					ArrayList<Integer> tempTTid = new ArrayList<Integer>();
					tempTTid.add(TTid);
					DSForJT.jobstoTT.put(tempMapTask, tempTTid);
				}
			}
			if(DSForJT.jobIdtoTask.containsKey(jsResponse.jobId)){
				DSForJT.jobIdtoTask.get(jsResponse.jobId).add(tempMapTask);
				//System.out.println("Adding job and map task : job ID ->" + tempMapTask.jobId + " Task id :" + tempMapTask.taskId);
				//System.out.println("MapTask added : " +DSForJT.jobIdtoTask.get(jsResponse.jobId).size());
			}
			else{
				HashSet<MapTaskInfo> tempJobList = new HashSet<MapTaskInfo>();
				tempJobList.add(tempMapTask);
				//System.out.println("Adding job and map task : job ID ->" + tempMapTask.jobId + " Task id :" + tempMapTask.taskId);
				DSForJT.jobIdtoTask.put(jsResponse.jobId, tempJobList);
				//System.out.println("MapTask added : " +DSForJT.jobIdtoTask.get(jsResponse.jobId).size());
			}
		}
		
		//startJob(jsResponse.jobId);
		return jsResponse.toProto();
	}

	@Override
	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	public byte[] getJobStatus(byte[] jobStatusRequest) {
		//Request from JobClient
		JobStatusRequest jsRequest = new JobStatusRequest(jobStatusRequest);
		JobStatusResponse jsResponse = new JobStatusResponse();
		int jobId = jsRequest.jobId;
//		System.out.println("Jobtracker Jobsubmit method jobId:" + jobId);
		if(DSForJT.jobIdtoJobresponse.containsKey(jobId)){
			if(DSForJT.reduceCompletedJobs.contains(jobId)){
				jsResponse.jobDone = true;
			}
			else{
				jsResponse.jobDone = false;
			}
			jsResponse.numMapTasksStarted = DSForJT.jobIdtoJobresponse.get(jobId).mapStarted;
			jsResponse.numReduceTasksStarted = DSForJT.jobIdtoJobresponse.get(jobId).reduceStarted;
			jsResponse.totalMapTasks = DSForJT.jobIdtoJobresponse.get(jobId).totalMap;
			jsResponse.totalReduceTasks = DSForJT.jobIdtoJobresponse.get(jobId).totalReduce;
			jsResponse.status = 1;
		}
		else{
			jsResponse.status = -1;
		}
		return jsResponse.toProto();
	}

	@Override
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	public byte[] heartBeat(byte[] heartBeatRequest) {
		MapReducePkg.MRRequestResponse.HeartBeatRequest taskTrackerHeatBeat = new MapReducePkg.MRRequestResponse.HeartBeatRequest(heartBeatRequest);
		int tId = taskTrackerHeatBeat.taskTrackerId;
		//System.out.println("Heart Beat received from task tracker ID " + tId );
		/*****************************************************************************/
		//Assign the MapTasks if Free slots for Map on current Task Tracker
		ArrayList<MapTaskInfo> mapTaskInfo = new ArrayList<MapTaskInfo>();
	//	System.out.println("Jobtracker HeartBeat method HearBeat recieved from TaskTracker Id:" + tId);
	//	System.out.println("Jobtracker HeartBeat method mapSlots free : " + taskTrackerHeatBeat.numMapSlotsFree);
		while(taskTrackerHeatBeat.numMapSlotsFree > 0 && DSForJT.TTtoJobs.containsKey(tId)){
			MapTaskInfo mpTask = new MapTaskInfo();
		//	System.out.println("Accessing TT ID :" + tId);
	//		if(DSForJT.TTtoJobs.get(tId).size()>0){
				mpTask = DSForJT.TTtoJobs.get(tId).get(0);
				mapTaskInfo.add(mpTask);
				DSForJT.TTtoJobs.get(tId).remove(0);
				ArrayList<Integer> tts = DSForJT.jobstoTT.get(mpTask);
				//System.out.println("JobTracker heartBeat jobs for TaskTracker :" + tId);
				for( int tt : tts ){
					DSForJT.TTtoJobs.get(tt).remove(mpTask);
					//System.out.println("Size :" + DSForJT.TTtoJobs.get(tt).size());
				}
				DSForJT.jobstoTT.remove(mpTask);
		/*	}
			else{
				DSForJT.TTtoJobs.remove(tId);
				break;
			}*/
			DSForJT.jobIdtoJobresponse.get(mpTask.jobId).mapStarted++;
			taskTrackerHeatBeat.numMapSlotsFree--;
			if(DSForJT.TTtoJobs.get(tId).size()==0){
				DSForJT.TTtoJobs.remove(tId);
				break;
			}
		}
		/*****************************************************************************/
		//Remove the completed MapTasks and add the reducerTask Info for that Map Task
		//ArrayList<MapTaskStatus> mpStatus = new ArrayList<MapTaskStatus>();
		for(MapTaskStatus mp : taskTrackerHeatBeat.mapStatus){
			if(mp.taskCompleted){
				//System.out.println("Jobtracker HeartBeat maptask Completed jobId: " + mp.jobId +"task id:" + mp.taskId);
				MapTaskInfo removeMap = new MapTaskInfo();
				removeMap.jobId = mp.jobId;
				removeMap.taskId = mp.taskId;
				if(DSForJT.jobIdtoTask.containsKey(mp.jobId)){
					//System.out.println("##########Size of MapTasks :" + DSForJT.jobIdtoTask.get(mp.jobId).size());
					DSForJT.jobIdtoTask.get(mp.jobId).remove(removeMap);
					//System.out.println("#############Size of MapTasks :" + DSForJT.jobIdtoTask.get(mp.jobId).size());
					if(DSForJT.jobIdtoTask.get(mp.jobId).size()==0){
						//System.out.println("Jobtracker HeartBeat All map tasks completed *************");
						DSForJT.mapCompletedJobs.add(mp.jobId);
						DSForJT.jobIdtoTask.remove(mp.jobId);
					}
				}
				String mapFile = new String("Job_"+mp.jobId + "_map_" + mp.taskId);
							
				if(DSForJT.jobIdtoReduceTask.containsKey(mp.jobId) ){
					System.out.println("Adding Map file to Reduce Task Info:" + mapFile);
					int last = DSForJT.jobIdtoReduceTask.get(mp.jobId).size()-1;
					if(last+1 == DSForJT.jobIdtoJobresponse.get(mp.jobId).totalReduce){
						DSForJT.jobIdtoReduceTask.get(mp.jobId).get(last).mapOutputFiles.add(mapFile);
					}
					else if(DSForJT.jobIdtoReduceTask.get(mp.jobId).get(last).mapOutputFiles.size() < DSForJT.jobIdtoJobresponse.get(mp.jobId).mapFileForEachReducer){
						DSForJT.jobIdtoReduceTask.get(mp.jobId).get(last).mapOutputFiles.add(mapFile);
					}
					else{
						ArrayList<String > tmpMapFiles = new ArrayList<String>();
						tmpMapFiles.add(mapFile);
						String rName = DSForJT.jobIdtoJobresponse.get(mp.jobId).reduceName;
						String oFile = new String("outputfile_" + mp.jobId +"_" + DSForJT.taskId++);
						ReducerTaskInfo reduceTaskInfo = new ReducerTaskInfo(mp.jobId,mp.taskId,rName,oFile,tmpMapFiles);
						DSForJT.jobIdtoReduceTask.get(mp.jobId).add(reduceTaskInfo);
					}
				}
				else{
					ArrayList<String > tmpMapFiles = new ArrayList<String>();
					tmpMapFiles.add(mapFile);
					String rName = DSForJT.jobIdtoJobresponse.get(mp.jobId).reduceName;
					String oFile = new String("outputfile_" + mp.jobId +"_" + DSForJT.taskId++);
					ReducerTaskInfo reduceTaskInfo = new ReducerTaskInfo(mp.jobId,mp.taskId,rName,oFile,tmpMapFiles);
					ArrayList<ReducerTaskInfo>  tempReduceList = new ArrayList<ReducerTaskInfo>();
					tempReduceList.add(reduceTaskInfo);
					DSForJT.jobIdtoReduceTask.put(mp.jobId,tempReduceList);
				}
			}
		}
		/*****************************************************************************/
		//Start the Reduce tasks if MapTasks are completed
		ArrayList<ReducerTaskInfo> reducerTaskInfo = new ArrayList<ReducerTaskInfo>();
	//	System.out.println("Jobtracker HeartBeat method ReduceSlots free : " + taskTrackerHeatBeat.numReduceSlotsFree);
		while(taskTrackerHeatBeat.numReduceSlotsFree>0 && DSForJT.mapCompletedJobs.size()>0){
				int jobId = DSForJT.mapCompletedJobs.first();
				System.out.println("Starting Reduce Job JobId:" + jobId);
				//System.out.println("JobTracker HeartBeat method Sendig to ReduceTask JobId:" + jobId);
				//System.out.println("^^^^^ ^^^^^ Number of Reducetask for job :" + DSForJT.jobIdtoReduceTask.get(jobId).size());
				/*for(ReducerTaskInfo r:DSForJT.jobIdtoReduceTask.get(jobId)){
					for(String n:r.mapOutputFiles){
					//System.out.println("^^^^ map FileName" + n);
					}
				}*/
				while(taskTrackerHeatBeat.numReduceSlotsFree>0 && DSForJT.jobIdtoReduceTask.containsKey(jobId)){
					System.out.println("JobTracker Starting reduce Task :" + jobId + " Starting reduce Task :"+ DSForJT.jobIdtoJobresponse.get(jobId).reduceStarted);
					System.out.println("JobTracker Reduce Task Left :" + DSForJT.jobIdtoReduceTask.get(jobId).size());
					if(DSForJT.jobIdtoReduceTask.get(jobId).size()>0){
							reducerTaskInfo.add(DSForJT.jobIdtoReduceTask.get(jobId).get(0));
							DSForJT.jobIdtoReduceTask.get(jobId).remove(0);
							taskTrackerHeatBeat.numReduceSlotsFree--;
							DSForJT.jobIdtoJobresponse.get(jobId).reduceStarted++;
							System.out.println("Update Reduce Task start Num :" + DSForJT.jobIdtoJobresponse.get(jobId).reduceStarted);
					}
					else{
						
						DSForJT.mapCompletedJobs.remove(jobId);
						DSForJT.jobIdtoReduceTask.remove(jobId);
						DSForJT.reduceCompletedJobs.add(jobId);
					}
				}
		}
		HeartBeatResponse hearBeatResponse = new HeartBeatResponse(1,mapTaskInfo,reducerTaskInfo);
		//System.out.println("Heartbeat Received");
		return hearBeatResponse.toProto();
	}
	
	void conf(String ConfFile){
		//NameNode IP,Port
		//TaskTracker ID,IP,Port(same as DataNode(Update TTidToTTLoc and TTLocToTTid)
		//JobTracker Port
		File conf = new File(ConfFile);
		if(!conf.exists()){
			//System.out.println("JobTracker Conf File does not exists please check the Path");
			System.exit(0);
		}
		//System.out.println("in JobTracker Conf");
		try{
			BufferedReader reader = new BufferedReader(new FileReader(conf));
			String line=null;
			while((line=reader.readLine()) != null){
				String []data = line.split("="); 
				switch(data[0]){
					case "NameNodeIp":
						nameNodeIP = data[1];
						break;
					case "NameNodePort":
						nameNodePort = Integer.parseInt(data[1]);
						break;
					case "TaskTrackerId":
						int Tid = Integer.parseInt(data[1]);
						IpConversion ipconv = new IpConversion();
						int ip,port;
						line = reader.readLine();
						data = line.split("=");
						ip = ipconv.packIP(Inet4Address.getByName(data[1]).getAddress());
						line = reader.readLine();
						data = line.split("=");
						port = Integer.parseInt(data[1]);
						DataNodeLocation dl = new DataNodeLocation(ip,port);
						if(!DSForJT.TTidToTTLoc.containsKey(Tid))
							DSForJT.TTidToTTLoc.put(Tid, dl);
						else{
							//System.out.println("JobTracker Conf Method TaskTracker Id already present");
						}
						if(!DSForJT.TTLocToTTid.containsKey(dl))
							DSForJT.TTLocToTTid.put(dl, Tid);
				
						else{
							//System.out.println("JobTracker Conf Method TaskTracker IP,port Already present");
						}
						break;
					case "JobTrackerIp":
						DSForJT.JobTrackerIp = data[1];
						break;
					case "JobTrackerPort":
						DSForJT.JobTrackerPort = Integer.parseInt(data[1]);
						break;
				}
			}
			reader.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	public static void main(String []args) throws UnknownHostException{
		//TODO conf file get locations of NameNode and other details
		if(args.length != 1){
			System.out.println("Invalid Usage");
			System.out.println("Usage java <JobTracker> <configFile Path>");
			System.exit(0);
		}
	
		//System.out.println("Get the Locations of TaskTrackers using Conf File");
		/*IpConversion ipconv = new IpConversion();
		int ip,port=5000;
		String tmp = "127.0.0.1";
		ip = ipconv.packIP(Inet4Address.getByName(tmp).getAddress());
		DataNodeLocation dl = new DataNodeLocation(ip,port);
		DSForJT.TTidToTTLoc.put(10, dl);
		DSForJT.TTLocToTTid.put(dl, 10);
		DataNodeLocation dls = new DataNodeLocation(ip,port); 
		System.out.println(ip + "  " + port);
		int TTid = DSForJT.TTLocToTTid.get(dls);*/
		try {
			//System.setProperty( "java.rmi.server.hostname", AllDataStructures.nameNodeIP ) ;
			
			JobTracker obj = new JobTracker(args[0]);
			//System.out.println("JobTracker Ip:" + DSForJT.JobTrackerIp + "  " + DSForJT.JobTrackerPort);
			Registry reg = LocateRegistry.createRegistry(DSForJT.JobTrackerPort);
			
			//obj.();
			reg.rebind("JobTracker", obj);
			System.out.println("JobTracker server is running");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Jobtracker Main method failure RMI registry");
		}
	}

}

class MapReduceSlots{
	int mapSlots,reduceSlots;
}
class JobIdTaskId{
	int jobId,taskId;
	public boolean equals(JobIdTaskId job){
		if(this.jobId == job.jobId && this.taskId == job.taskId)
		return true;
		else return false;
	}
}
class JobResponseData{
	int mapFileForEachReducer,totalMap,totalReduce;
	int mapStarted,reduceStarted;
	String reduceName;
}
class DSForJT{
	public static int jobId=0,taskId=0;
	public static int JobTrackerPort;
	public static String JobTrackerIp;
//	JonID to TaskTracker Map
	//public static HashMap<Integer,ArrayList<BlockLocations> > jobIdtoTaskTracker = new HashMap<Integer,ArrayList<BlockLocations> >();
	//TaskTracker Id to Number of slots HashMap<TaskTrackerId,NumberOfSlots>
	//public static HashMap<Integer, MapReduceSlots> taskTrackerSlots = new HashMap<Integer,MapReduceSlots>();

	public static ConcurrentHashMap<Integer,DataNodeLocation> TTidToTTLoc = new ConcurrentHashMap<Integer,DataNodeLocation>();
	public static ConcurrentHashMap<DataNodeLocation,Integer> TTLocToTTid = new ConcurrentHashMap<DataNodeLocation,Integer>();
	//HashMap contains Task Tracker Id to MaptaskInfo(JobId,MapTaskId) arrayList 
	public static ConcurrentHashMap<Integer, ArrayList<MapTaskInfo>> TTtoJobs = new ConcurrentHashMap<Integer,ArrayList<MapTaskInfo>>();
	//HashMap MapTaskInfo(jobId,TaskId) to TaskTracker Id arraylist
	public static ConcurrentHashMap<MapTaskInfo, ArrayList<Integer>> jobstoTT = new ConcurrentHashMap<MapTaskInfo,ArrayList<Integer>>();
	//HashMap JobId to MapTaskInfo(jobId,TaskId) to check map task completion
	public static ConcurrentHashMap<Integer, HashSet<MapTaskInfo>> jobIdtoTask = new ConcurrentHashMap<Integer, HashSet<MapTaskInfo>>();
	//HashMap JobId to ReduceTaskInfo arrayList this contains the ReduceTask list to passed to TT after completion of all MapTasks
	public static ConcurrentHashMap<Integer,ArrayList<ReducerTaskInfo>> jobIdtoReduceTask = new ConcurrentHashMap<Integer, ArrayList<ReducerTaskInfo>>();
	//HashMap JobId to ReducerName and Number of Files to one reducer Task Info 
	public static ConcurrentHashMap<Integer,JobResponseData> jobIdtoJobresponse = new ConcurrentHashMap<Integer,JobResponseData>();
	//TreeSet jobId to Completed Maptasks
	public static TreeSet<Integer> mapCompletedJobs = new TreeSet<Integer>();
	public static TreeSet<Integer> reduceCompletedJobs = new TreeSet<Integer>();
}