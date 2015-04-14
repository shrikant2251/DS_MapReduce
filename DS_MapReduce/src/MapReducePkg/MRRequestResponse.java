package MapReducePkg;
import java.util.ArrayList;

import com.google.protobuf.InvalidProtocolBufferException;

public interface MRRequestResponse {
	public class JobSubmitRequest{
		public String mapName,reduceName,inputFile,outputFile;
		public int numReduceTasks;
		public JobSubmitRequest() {
			// TODO Auto-generated constructor stub
		}
		public JobSubmitRequest(String m,String r,String i,String o,int n) {
			// TODO Auto-generated constructor stub
			mapName = m;
			reduceName = r;
			inputFile = i;
			outputFile = o;
			numReduceTasks = n;
		}
		public JobSubmitRequest(byte []input){
			MapReduce.JobSubmitRequest builder = null;
			try{
				builder = MapReduce.JobSubmitRequest.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			mapName = builder.getMapName();
			reduceName = builder.getReducerName();
			inputFile = builder.getInputFile();
			outputFile = builder.getOutputFile();
			numReduceTasks = builder.getNumReduceTasks();
		}
		public byte[] toProto(){
			MapReduce.JobSubmitRequest.Builder builder = MapReduce.JobSubmitRequest.newBuilder();
			builder.setMapName(mapName);
			builder.setReducerName(reduceName);
			builder.setInputFile(inputFile);
			builder.setOutputFile(outputFile);
			builder.setNumReduceTasks(numReduceTasks);
			return builder.build().toByteArray();
		}
	}
	
	public class JobSubmitResponse{
		public int status,jobId;
		public JobSubmitResponse() {
			// TODO Auto-generated constructor stub
		}
		public JobSubmitResponse(int s,int j){
			status = s;
			jobId = j;
		}
		public JobSubmitResponse(byte []input){
			MapReduce.JobSubmitResponse builder = null;
			try{
				builder = MapReduce.JobSubmitResponse.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			status = builder.getStatus();
			jobId = builder.getJobId();
		}
		public byte []toProto(){
			MapReduce.JobSubmitResponse.Builder builder =  MapReduce.JobSubmitResponse.newBuilder();
			builder.setStatus(status);
			builder.setJobId(jobId);
			return builder.build().toByteArray();
		}
	}
	public class JobStatusRequest{
		public int jobId;
		public JobStatusRequest(){
			
		}
		public JobStatusRequest(int j){
			jobId = j;
		}
		public JobStatusRequest(byte []input){
			MapReduce.JobStatusRequest builder = null;
			try{
				builder = MapReduce.JobStatusRequest.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			jobId = builder.getJobId();
		}
		public byte[] toProto(){
			MapReduce.JobStatusRequest.Builder builder = MapReduce.JobStatusRequest.newBuilder();
			builder.setJobId(jobId);
			return builder.build().toByteArray();
		}
	}
	public class JobStatusResponse{
		public int status,totalMapTasks,numMapTasksStarted,totalReduceTasks,numReduceTasksStarted;
		public boolean jobDone;
		public JobStatusResponse(){
			
		}
		public JobStatusResponse(int s,int tmt,int nmts,int trt,int nrts,boolean jd){
			status = s;
			totalMapTasks = tmt;
			numMapTasksStarted = nmts;
			totalReduceTasks = trt;
			numReduceTasksStarted = nrts;
			jobDone = jd;
		}
		public JobStatusResponse(byte []input){
			MapReduce.JobStatusResponse builder = null;
			try{
				builder = MapReduce.JobStatusResponse.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			status = builder.getStatus();
			totalMapTasks = builder.getTotalMapTasks();
			totalReduceTasks = builder.getTotalReduceTasks();
			numMapTasksStarted = builder.getNumMapTasksStarted();
			numReduceTasksStarted = builder.getNumReduceTasksStarted();
			jobDone = builder.getJobDone();
		}
		public byte[] toProto(){
			MapReduce.JobStatusResponse.Builder builder = MapReduce.JobStatusResponse.newBuilder();
			builder.setStatus(status);
			builder.setTotalMapTasks(totalMapTasks);
			builder.setNumMapTasksStarted(numMapTasksStarted);
			builder.setNumReduceTasksStarted(numReduceTasksStarted);
			builder.setTotalReduceTasks(totalReduceTasks);
			builder.setJobDone(jobDone);
			return builder.build().toByteArray();
		}
	}
	public class MapTaskStatus{
		public int jobId,taskId;
		public boolean taskCompleted;
		public String mapOutputFile;
		public MapTaskStatus(){
			
		}
		public MapTaskStatus(int j,int t,boolean tc,String mapOf){
			jobId = j;
			taskId = t;
			taskCompleted = tc;
			mapOutputFile = mapOf;
		}
		public MapTaskStatus(byte []input){
			MapReduce.MapTaskStatus builder = null;
			try{
				builder = MapReduce.MapTaskStatus.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			jobId = builder.getJobId();
			taskId = builder.getTaskId();
			taskCompleted = builder.getTaskCompleted();
			mapOutputFile = builder.getMapOutputFile();
		}
		public MapTaskStatus(MapReduce.MapTaskStatus mTaskStatus){
			
			jobId = mTaskStatus.getJobId();
			taskId = mTaskStatus.getTaskId();
			taskCompleted = mTaskStatus.getTaskCompleted();
			mapOutputFile = mTaskStatus.getMapOutputFile();
		}
		public MapReduce.MapTaskStatus.Builder toProtoObject(){
			MapReduce.MapTaskStatus.Builder builder = MapReduce.MapTaskStatus.newBuilder();
			builder.setJobId(jobId);
			builder.setTaskId(taskId);
			builder.setTaskCompleted(taskCompleted);
			builder.setMapOutputFile(mapOutputFile);
			return builder;
		}
		public byte[] toProto(){
			MapReduce.MapTaskStatus.Builder builder = MapReduce.MapTaskStatus.newBuilder();
			builder.setJobId(jobId);
			builder.setTaskId(taskId);
			builder.setTaskCompleted(taskCompleted);
			builder.setMapOutputFile(mapOutputFile);
			return builder.build().toByteArray(); 
		}
	}
	public class ReduceTaskStatus{
		public int jobId,taskId;
		public boolean taskCompleted;
		public ReduceTaskStatus(){
			
		}
		public ReduceTaskStatus(int j,int t,boolean tc){
			jobId = j;
			taskId = t;
			taskCompleted = tc;
		}
		public ReduceTaskStatus(byte []input){
			MapReduce.ReduceTaskStatus builder = null;
			try{
				builder = MapReduce.ReduceTaskStatus.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			jobId = builder.getJobId();
			taskId = builder.getTaskId();
			taskCompleted = builder.getTaskCompleted();
		}
		public ReduceTaskStatus(MapReduce.ReduceTaskStatus rTaskStatus){
			jobId = rTaskStatus.getJobId();
			taskId = rTaskStatus.getTaskId();
			taskCompleted = rTaskStatus.getTaskCompleted();
		}
		public MapReduce.ReduceTaskStatus.Builder toProtoObject(){
			MapReduce.ReduceTaskStatus.Builder builder = MapReduce.ReduceTaskStatus.newBuilder();
			builder.setJobId(jobId);
			builder.setTaskId(taskId);
			builder.setTaskCompleted(taskCompleted);
			return builder;
		}
		public byte[] toProto(){
			MapReduce.ReduceTaskStatus.Builder builder = MapReduce.ReduceTaskStatus.newBuilder();
			builder.setJobId(jobId);
			builder.setTaskId(taskId);
			builder.setTaskCompleted(taskCompleted);
			return builder.build().toByteArray(); 
		}
	}
	public class HeartBeatRequest{
		public int taskTrackerId,numMapSlotsFree,numReduceSlotsFree;
		public ArrayList<MapTaskStatus> mapStatus;
		public ArrayList<ReduceTaskStatus> reduceStatus;
		public HeartBeatRequest(){
			
		}
		public HeartBeatRequest(int ttI,int nMSF,int nRSF,ArrayList<MapTaskStatus> ms,ArrayList<ReduceTaskStatus> rs){
			taskTrackerId = ttI;
			numMapSlotsFree = nMSF;
			numReduceSlotsFree = nRSF;
			mapStatus = new ArrayList<MapTaskStatus>();
			for(MapTaskStatus st : ms){
				mapStatus.add(st);
			}
			reduceStatus = new ArrayList<ReduceTaskStatus>();
			for(ReduceTaskStatus st : rs){
				reduceStatus.add(st);
			}
		}
		public HeartBeatRequest(byte []input){
			MapReduce.HeartBeatRequest builder = null;
			try{
				builder = MapReduce.HeartBeatRequest.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			taskTrackerId = builder.getTaskTrackerId();
			numMapSlotsFree = builder.getNumMapSlotsFree();
			numReduceSlotsFree = builder.getNumReduceSlotsFree();
			mapStatus = new ArrayList<MapTaskStatus>();
			for(MapReduce.MapTaskStatus st : builder.getMapStatusList()){
					mapStatus.add(new MapTaskStatus(st));
			}
			reduceStatus = new ArrayList<ReduceTaskStatus>();
			for(MapReduce.ReduceTaskStatus st : builder.getReduceStatusList()){
					reduceStatus.add(new ReduceTaskStatus(st));
			}
		}
		public byte[] toProto(){
			MapReduce.HeartBeatRequest.Builder builder = MapReduce.HeartBeatRequest.newBuilder();
			builder.setTaskTrackerId(taskTrackerId);
			builder.setNumMapSlotsFree(numMapSlotsFree);
			builder.setNumReduceSlotsFree(numReduceSlotsFree);
			for(MapTaskStatus mTask : mapStatus ){
				builder.addMapStatus(mTask.toProtoObject());

			}
			for(ReduceTaskStatus rTask : reduceStatus ){
				builder.addReduceStatus(rTask.toProtoObject());
			}
			return builder.build().toByteArray();
		}
	}
	public class MapTaskInfo{
		public int jobId,taskId,inputBlocks;
		public String mapName;
		public int hashCode(){
			return 1;
		}
		public boolean equals(Object obj){
			MapTaskInfo mapTask = (MapTaskInfo)obj;
			if(this.jobId == mapTask.jobId && this.taskId == mapTask.taskId)
			return true;
			else return false;
		}
		public MapTaskInfo(){
			
		}
		public MapTaskInfo( MapReduce.MapTaskInfo mTask){
			jobId = mTask.getJobId();
			taskId = mTask.getTaskId();
			inputBlocks = mTask.getInputBlocks();
			mapName = mTask.getMapName();
		}
		public MapTaskInfo(int j,int t,int ib,String mn){
			jobId = j;
			taskId = t;
			inputBlocks = ib;
			mapName = mn;
		}
		public MapTaskInfo(byte []input){
			MapReduce.MapTaskInfo builder = null;
			try{
				builder = MapReduce.MapTaskInfo.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			jobId = builder.getJobId();
			taskId = builder.getTaskId();
			inputBlocks = builder.getInputBlocks();
			mapName = builder.getMapName();
		}
		public MapReduce.MapTaskInfo.Builder toProtoObject(){
			MapReduce.MapTaskInfo.Builder taskBuilder = MapReduce.MapTaskInfo.newBuilder();
			taskBuilder.setJobId(jobId);
			taskBuilder.setTaskId(taskId);
			taskBuilder.setInputBlocks(inputBlocks);
			taskBuilder.setMapName(mapName);
			return taskBuilder;
		}
		public byte[]toProto(){
			MapReduce.MapTaskInfo.Builder builder = MapReduce.MapTaskInfo.newBuilder();
			builder.setJobId(jobId);
			builder.setTaskId(taskId);
			builder.setInputBlocks(inputBlocks);
			builder.setMapName(mapName);
			return builder.build().toByteArray();
		}
	}
	public class ReducerTaskInfo{
		public int jobId,taskId;
		public String reducerName,outputFile;
		public ArrayList<String> mapOutputFiles;
		public ReducerTaskInfo(){
			
		}
		public ReducerTaskInfo(int j,int t,String rName,String OFile,ArrayList<String> mapOFiles){
			jobId = j;
			taskId = t;
			reducerName = rName;
			outputFile = OFile;
			mapOutputFiles = new ArrayList<String>();
			for(String file : mapOFiles){
				mapOutputFiles.add(file);
			}
		}
		public ReducerTaskInfo(MapReduce.ReducerTaskInfo rTask){
			jobId = rTask.getJobId();
			taskId = rTask.getTaskId();
			reducerName = rTask.getReducerName();
			outputFile = rTask.getOutputFile();
			mapOutputFiles = new ArrayList<String>();
			for(String  file : rTask.getMapOutputFilesList()){
				mapOutputFiles.add(file);
			}
		}
		public MapReduce.ReducerTaskInfo.Builder toProtoObject(){
			MapReduce.ReducerTaskInfo.Builder taskBuilder = MapReduce.ReducerTaskInfo.newBuilder();
			taskBuilder.setJobId(jobId);
			taskBuilder.setTaskId(taskId);
			taskBuilder.setReducerName(reducerName);
			taskBuilder.setOutputFile(outputFile);
			mapOutputFiles = new ArrayList<String>();
			for(String  file : taskBuilder.getMapOutputFilesList()){
				mapOutputFiles.add(file);
			}
			return taskBuilder;
		}
		public ReducerTaskInfo(byte []input){
			MapReduce.ReducerTaskInfo builder = null;
			try{
				builder = MapReduce.ReducerTaskInfo.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			jobId = builder.getJobId();
			taskId = builder.getTaskId();
			reducerName = builder.getReducerName();
			outputFile = builder.getOutputFile();
			mapOutputFiles = new ArrayList<String>();
			for(String file : builder.getMapOutputFilesList()){
				mapOutputFiles.add(file);
			}
		}
		public byte[] toProto(){
			MapReduce.ReducerTaskInfo.Builder builder = MapReduce.ReducerTaskInfo.newBuilder();
			builder.setJobId(jobId);
			builder.setTaskId(taskId);
			builder.setReducerName(reducerName);
			builder.setOutputFile(outputFile);
			int idx = 0;
			for(String file : mapOutputFiles){
				builder.setMapOutputFiles(idx, file);
				idx++;
			}
			return builder.build().toByteArray();
		}
	}
	public class HeartBeatResponse{
		int status;
		public ArrayList<MapTaskInfo> mapTasks;
		public ArrayList<ReducerTaskInfo> reduceTasks;
		public HeartBeatResponse(){
			
		}
		public HeartBeatResponse(int s,ArrayList<MapTaskInfo> mt,ArrayList<ReducerTaskInfo> rt){
			status = s;
			mapTasks = new ArrayList<MapTaskInfo>();
			for(MapTaskInfo mTask : mt){
				mapTasks.add(mTask);
			}
			reduceTasks = new ArrayList<ReducerTaskInfo>();
			for(ReducerTaskInfo rTask : rt){
				reduceTasks.add(rTask);
			}
		}
		public HeartBeatResponse(byte []input){
			MapReduce.HeartBeatResponse builder = null;
			try{
				builder = MapReduce.HeartBeatResponse.parseFrom(input);
			}
			catch(InvalidProtocolBufferException e){
				e.printStackTrace();
			}
			status = builder.getStatus();
			mapTasks = new ArrayList<MapTaskInfo>();
			for(MapReduce.MapTaskInfo mTask : builder.getMapTasksList()){
				mapTasks.add(new MapTaskInfo(mTask));
			}
			reduceTasks = new ArrayList<ReducerTaskInfo>();
			for(MapReduce.ReducerTaskInfo rTask : builder.getReduceTasksList()){
				reduceTasks.add(new ReducerTaskInfo(rTask));
			}
			
		}
		public byte[] toProto(){
			MapReduce.HeartBeatResponse.Builder builder = MapReduce.HeartBeatResponse.newBuilder();
			builder.setStatus(status);
			for(MapTaskInfo mTask : mapTasks){
				builder.addMapTasks(mTask.toProtoObject());
			}
			
			for(ReducerTaskInfo rTask : reduceTasks){
				builder.addReduceTasks(rTask.toProtoObject());
			}
			return builder.build().toByteArray(); 
		}
	}
}
