import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;

import HDFSPackage.INameNode;
import HDFSPackage.RequestResponse.*;
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
	void startJob(int jobId){
		ArrayList<BlockLocations> blocks = DataStructuresForJobTracker.jobIdtoTaskTracker.get(jobId);
		for(BlockLocations blk : blocks){
			ArrayList<DataNodeLocation> taskTrackerLocation = new ArrayList<DataNodeLocation>();
			for(int i=0;i<taskTrackerLocation.size();i++){
				int tId = DataStructuresForJobTracker.TaskTrackerToTTid.get(taskTrackerLocation.get(i));
				if(DataStructuresForJobTracker.taskTrackerSlots.get(tId).mapSlots > 0){
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
	}
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
		jsResponse.jobId = DataStructuresForJobTracker.jobId++;
		//TODO Keep HashMap of JobId to TaskTrackers or blockLocations used later in getJobStatus
		DataStructuresForJobTracker.jobIdtoTaskTracker.put(jsResponse.jobId, blockLocations);
		
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
		if(DataStructuresForJobTracker.taskTrackerSlots.containsKey(taskTrackerHeatBeat.taskTrackerId)){
			DataStructuresForJobTracker.taskTrackerSlots.get(taskTrackerHeatBeat.taskTrackerId).mapSlots = taskTrackerHeatBeat.numMapSlotsFree;
			DataStructuresForJobTracker.taskTrackerSlots.get(taskTrackerHeatBeat.taskTrackerId).reduceSlots =  taskTrackerHeatBeat.numReduceSlotsFree;
		}
		else{
			MapReduceSlots tempSlot = new MapReduceSlots();
			tempSlot.mapSlots = taskTrackerHeatBeat.numMapSlotsFree;
			tempSlot.reduceSlots = taskTrackerHeatBeat.numReduceSlotsFree;
			DataStructuresForJobTracker.taskTrackerSlots.put(taskTrackerHeatBeat.taskTrackerId,tempSlot);
		}
			
		return null;
	}
	
	public static void main(String []args){
		//TODO conf file get locations of NameNode and other details
		try {
			//System.setProperty( "java.rmi.server.hostname", AllDataStructures.nameNodeIP ) ;
			Registry reg = LocateRegistry.createRegistry(DataStructuresForJobTracker.JobTrackerPort);
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
class DataStructuresForJobTracker{
	public static int jobId=0;
	public static int JobTrackerPort = 2000;
//	JonID to TaskTracker Map
	public static HashMap<Integer,ArrayList<BlockLocations> > jobIdtoTaskTracker = new HashMap<Integer,ArrayList<BlockLocations> >();
	//TaskTracker Id to Number of slots HashMap<TaskTrackerId,NumberOfSlots>
	public static HashMap<Integer, MapReduceSlots> taskTrackerSlots = new HashMap<Integer,MapReduceSlots>();

	public static HashMap<Integer,DataNodeLocation> TTidToTaskTracker = new HashMap<Integer,DataNodeLocation>();
	public static HashMap<DataNodeLocation,Integer> TaskTrackerToTTid = new HashMap<DataNodeLocation,Integer>();
	
}