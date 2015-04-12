import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

import HDFSPackage.INameNode;
import HDFSPackage.RequestResponse.*;
import MapReducePkg.MRRequestResponse.HeartBeatResponse;
import MapReducePkg.MRRequestResponse.ReducerTaskInfo;
import MapReducePkg.MRRequestResponse.*;

public class JobTracker implements IJobTracker{


	public static String nameNodeIP;
	public static int nameNodePort,blockSize;
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
		} catch (Exception e) {
			status = -1;
			e.printStackTrace();
		}
		if (status == -1) {
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
		try {
			Registry myreg = LocateRegistry.getRegistry(nameNodeIP,nameNodePort);
			in = (INameNode) myreg.lookup("NameNode");
		} catch (Exception e) {
			//RMIStatus = -1;
			System.out.println("GeneralClient :: RMI Error locating NameNode Registry.");
			e.printStackTrace();
			System.out.println("JobTracker Method getBlockLocations Failure due to RMI call to NameNode failed");
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
			System.out.println("GeneralClient :: Error retrieving block locations.");
			e1.printStackTrace();
			//return status;
			System.out.println("JobTracker Method getBlockLocations Failure due to getBlockLocations failed");
			return null;
		}
		
		BlockLocationResponse blockLocationResponse = new BlockLocationResponse(getBlockLocationResponse);
		if (blockLocationResponse.status == -1) {
			System.out.println("JobTracker Method getBlockLocations Failure due to BlockLocationResponse failed");
			return null;
		}
		return blockLocationResponse.blockLocations;
	}
	/*void startJob(int jobId){
		ArrayList<BlockLocations> blocks = DSForJT.jobIdtoTaskTracker.get(jobId);
		for(BlockLocations blk : blocks){
			ArrayList<DataNodeLocation> taskTrackerLocation = new ArrayList<DataNodeLocation>();
			for(int i=0;i<taskTrackerLocation.size();i++){
				int tId = DSForJT.TaskTrackerToTTid.get(taskTrackerLocation.get(i));
				if(DSForJT.taskTrackerSlots.get(tId).mapSlots > 0){
					//TODO call the mapTask of TaskTracker
					//Assign TaskId and send the map Task to TaskTracker
					break;
				}
				if((i+1) == taskTrackerLocation.size()){
					//TODO call the mapTask of TaskTracker
					//Assign TaskId and send  map Task to TaskTracker
					break;
				}
			}
		}
	}*/
	@Override
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	public byte[] jobSubmit(byte[] jobSubmitRequest) {
		// TODO Auto-generated method stub
		JobSubmitResponse jsResponse = new JobSubmitResponse();
		JobSubmitRequest jsRequest = new JobSubmitRequest(jobSubmitRequest);
		
		// opens the file in HDFS in Read Mode
		byte[] openResponse;
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
 
		//TODO Number of Maptask equal to number of blockLocations obtained
		
		jsResponse.status = 1;
		jsResponse.jobId = DSForJT.jobId++;
		//TODO Keep HashMap of JobId to TaskTrackers or blockLocations used later in getJobStatus
		DSForJT.jobIdtoTaskTracker.put(jsResponse.jobId, blockLocations);
		JobResponseData tempRed = new JobResponseData();
		tempRed.reduceName = jsRequest.reduceName;
		double d1 = blockLocations.size();
		double d2 = jsRequest.numReduceTasks;
		tempRed.mapFileForEachReducer = (int)(d1/ d2);
		tempRed.totalMap = blockLocations.size();
		tempRed.totalReduce = jsRequest.numReduceTasks;
		tempRed.mapStarted = 0;
		tempRed.reduceStarted = 0;
		DSForJT.jobIdtoJobresponse.put(jsResponse.jobId, tempRed);
		for(BlockLocations block : blockLocations){
			MapTaskInfo tempMapTask = new MapTaskInfo();
			tempMapTask.jobId = jsResponse.jobId;
			tempMapTask.taskId = DSForJT.taskId++;
			tempMapTask.mapName = jsRequest.mapName;
			tempMapTask.inputBlocks = block.blockNumber;
			for(DataNodeLocation dls : block.locations){
				int TTid = DSForJT.TTLocToTTid.get(dls);
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
			}
			else{
				HashSet<MapTaskInfo> tempJobList = new HashSet<MapTaskInfo>();
				tempJobList.add(tempMapTask);
				DSForJT.jobIdtoTask.put(jsResponse.jobId, tempJobList);
			}
		}
		
		//startJob(jsResponse.jobId);
		return jsResponse.toProto();
	}

	@Override
	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	public byte[] getJobStatus(byte[] jobStatusRequest) {
		// TODO Auto-generated method stub
		//Request from JobClient 
		return null;
	}

	@Override
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	public byte[] heartBeat(byte[] heartBeatRequest) {
		// TODO Auto-generated method stub
//		 TODO : When a TT heartbeats, it uses the number of map/reduce slots available to
//		decide if it can schedule tasks on the TT. The HB response contains information required to execute the map/reduce tasks.
//		The heartbeat from TT also contains information about the status of the tasks
		MapReducePkg.MRRequestResponse.HeartBeatRequest taskTrackerHeatBeat = new MapReducePkg.MRRequestResponse.HeartBeatRequest(heartBeatRequest);
		int tId = taskTrackerHeatBeat.taskTrackerId;
		/*****************************************************************************/
		//Assign the MapTasks if Free slots for Map on current Task Tracker
		ArrayList<MapTaskInfo> mapTaskInfo = new ArrayList<MapTaskInfo>();
		while(taskTrackerHeatBeat.numMapSlotsFree > 0 && DSForJT.TTtoJobs.containsKey(tId)){
			MapTaskInfo mpTask = new MapTaskInfo();
			mpTask = DSForJT.TTtoJobs.get(tId).get(0);
			mapTaskInfo.add(mpTask);
			DSForJT.TTtoJobs.get(tId).remove(0);
			ArrayList<Integer> tts = DSForJT.jobstoTT.get(mpTask);
			for( int tt : tts ){
				DSForJT.TTtoJobs.get(tt).remove(mpTask);
			}
			DSForJT.jobstoTT.remove(mpTask);
			if(DSForJT.TTtoJobs.get(tId).size()==0){
				DSForJT.TTtoJobs.remove(tId);
			}
			DSForJT.jobIdtoJobresponse.get(mpTask.jobId).mapStarted++;
			taskTrackerHeatBeat.numMapSlotsFree--;
		}
		/*****************************************************************************/
		//Remove the completed MapTasks and add the reducerTask Info for that Map Task
		//ArrayList<MapTaskStatus> mpStatus = new ArrayList<MapTaskStatus>();
		for(MapTaskStatus mp : taskTrackerHeatBeat.mapStatus){
			if(mp.taskCompleted){
				MapTaskInfo removeMap = new MapTaskInfo();
				removeMap.jobId = mp.jobId;
				removeMap.taskId = mp.taskId;
				if(DSForJT.jobIdtoTask.containsKey(mp.jobId)){
					DSForJT.jobIdtoTask.remove(removeMap);
					if(DSForJT.jobIdtoTask.get(mp.jobId).size()==0){
						DSForJT.mapCompletedJobs.add(mp.jobId);
						DSForJT.jobIdtoTask.remove(mp.jobId);
					}
				}
				String mapFile = new String("job_"+mp.jobId + "_map" + mp.taskId);
				ReducerTaskInfo reduceTaskInfo = new ReducerTaskInfo();
				reduceTaskInfo.jobId = mp.jobId;
				reduceTaskInfo.taskId = mp.taskId;
				reduceTaskInfo.reducerName = DSForJT.jobIdtoJobresponse.get(mp.jobId).reduceName;
				reduceTaskInfo.mapOutputFiles = new ArrayList<String>();
				reduceTaskInfo.mapOutputFiles.add(mapFile);
				reduceTaskInfo.outputFile = new String("outputfile_" + mp.jobId +"_" + DSForJT.reduceId++);
				
				if(DSForJT.jobIdtoReduceTask.containsKey(mp.jobId) ){
					int last = DSForJT.jobIdtoReduceTask.get(mp.jobId).size()-1;
					if(last+1 == DSForJT.jobIdtoJobresponse.get(mp.jobId).totalReduce){
						DSForJT.jobIdtoReduceTask.get(mp.jobId).get(last).mapOutputFiles.add(mapFile);
					}
					else if(DSForJT.jobIdtoReduceTask.get(mp.jobId).get(last).mapOutputFiles.size() < DSForJT.jobIdtoJobresponse.get(mp.jobId).mapFileForEachReducer){
						DSForJT.jobIdtoReduceTask.get(mp.jobId).get(last).mapOutputFiles.add(mapFile);
					}
					else{
						
						DSForJT.jobIdtoReduceTask.get(mp.jobId).add(reduceTaskInfo);
					}
				}
				else{
					ArrayList<ReducerTaskInfo> tempReduceList = new ArrayList<ReducerTaskInfo>();
					DSForJT.jobIdtoReduceTask.put(mp.jobId,tempReduceList);
					DSForJT.jobIdtoReduceTask.get(mp.jobId).add(reduceTaskInfo);
				}
			}
		}
		/*****************************************************************************/
		//Start the Reduce tasks if MapTasks are completed
		ArrayList<ReducerTaskInfo> reducerTaskInfo = new ArrayList<ReducerTaskInfo>();
		while(taskTrackerHeatBeat.numReduceSlotsFree>0 && DSForJT.mapCompletedJobs.size()>0){
				int jobId = DSForJT.mapCompletedJobs.first();
				while(taskTrackerHeatBeat.numReduceSlotsFree>0 && DSForJT.jobIdtoReduceTask.containsKey(jobId)){
					if(DSForJT.jobIdtoReduceTask.get(jobId).size()>0){
							reducerTaskInfo.add(DSForJT.jobIdtoReduceTask.get(jobId).get(0));
							DSForJT.jobIdtoReduceTask.get(jobId).remove(0);
							taskTrackerHeatBeat.numReduceSlotsFree--;
							DSForJT.jobIdtoJobresponse.get(jobId).reduceStarted++;
					}
					else{
						DSForJT.mapCompletedJobs.remove(jobId);
						DSForJT.jobIdtoReduceTask.remove(jobId);
						DSForJT.reduceCompletedJobs.add(jobId);
					}
				}
		}
		HeartBeatResponse hearBeatResponse = new HeartBeatResponse(1,mapTaskInfo,reducerTaskInfo);
		return hearBeatResponse.toProto();
	}
	
	public static void main(String []args){
		//TODO conf file get locations of NameNode and other details
		try {
			//System.setProperty( "java.rmi.server.hostname", AllDataStructures.nameNodeIP ) ;
			Registry reg = LocateRegistry.createRegistry(DSForJT.JobTrackerPort);
			JobTracker obj = new JobTracker();
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
	public static int jobId=0,taskId=0,reduceId=0;
	public static int JobTrackerPort = 2000;
//	JonID to TaskTracker Map
	public static HashMap<Integer,ArrayList<BlockLocations> > jobIdtoTaskTracker = new HashMap<Integer,ArrayList<BlockLocations> >();
	//TaskTracker Id to Number of slots HashMap<TaskTrackerId,NumberOfSlots>
	//public static HashMap<Integer, MapReduceSlots> taskTrackerSlots = new HashMap<Integer,MapReduceSlots>();

	public static HashMap<Integer,DataNodeLocation> TTidToTTLoc = new HashMap<Integer,DataNodeLocation>();
	public static HashMap<DataNodeLocation,Integer> TTLocToTTid = new HashMap<DataNodeLocation,Integer>();
	//HashMap contains Task Tracker Id to MaptaskInfo(JobId,MapTaskId) arrayList 
	public static HashMap<Integer, ArrayList<MapTaskInfo>> TTtoJobs = new HashMap<Integer,ArrayList<MapTaskInfo>>();
	//HashMap MapTaskInfo(jobId,TaskId) to TaskTracker Id arraylist
	public static HashMap<MapTaskInfo, ArrayList<Integer>> jobstoTT = new HashMap<MapTaskInfo,ArrayList<Integer>>();
	//HashMap JobId to MapTaskInfo(jobId,TaskId) to check map task completion
	public static HashMap<Integer, HashSet<MapTaskInfo>> jobIdtoTask = new HashMap<Integer, HashSet<MapTaskInfo>>();
	//HashMap JobId to ReduceTaskInfo arrayList this contains the ReduceTask list to passed to TT after completion of all MapTasks
	public static HashMap<Integer,ArrayList<ReducerTaskInfo>> jobIdtoReduceTask = new HashMap<Integer, ArrayList<ReducerTaskInfo>>();
	//HashMap JobId to ReducerName and Number of Files to one reducer Task Info 
	public static HashMap<Integer,JobResponseData> jobIdtoJobresponse = new HashMap<Integer,JobResponseData>();
	//TreeSet jobId to Completed Maptasks
	public static TreeSet<Integer> mapCompletedJobs = new TreeSet<Integer>();
	public static TreeSet<Integer> reduceCompletedJobs = new TreeSet<Integer>();
}