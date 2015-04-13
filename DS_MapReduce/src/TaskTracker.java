import java.awt.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import MapReducePkg.MRRequestResponse.HeartBeatRequest;
import MapReducePkg.MRRequestResponse.HeartBeatResponse;
import MapReducePkg.MRRequestResponse.MapTaskInfo;
import MapReducePkg.MRRequestResponse.MapTaskStatus;
import MapReducePkg.MRRequestResponse.ReduceTaskStatus;
import MapReducePkg.MRRequestResponse.ReducerTaskInfo;

public class TaskTracker extends TimerTask{
	
	static String JobTrackerIp="127.0.0.1";
	static int JobTrackerPort=2000;
	static int noOfMapThreads,noOfReduceThreads,taskTrackerId;
	static int noOfMapThreadsRemain,noOfReduceThreadsRemain;
	static int heartBeatRate;
	static HashMap<MapTaskInfo,Future<?>> mapTracker ;
	static HashMap<ReducerTaskInfo,Future<?>> reduceTracker;
	static ExecutorService mapThreadPool,reduceThreadPool;
	
	// TODO When the TT gets a map request, it places the request in an internal
//	queue. The consumers of this queue are a fixed number of threads
	//TODO  Threadpool creation "http://tutorials.jenkov.com/java-concurrency/thread-pools.html"
	//TODO take input from conf file 
	//TaskTracker ID,Jobtracker Ip,Port,Number of Map threads,Number of reduce threads

	public TaskTracker(String configFilePath)
	{
		super();
		config(configFilePath);
		mapTracker = new HashMap<MapTaskInfo,Future<?>>();
		reduceTracker = new HashMap<ReducerTaskInfo,Future<?>>();
		mapThreadPool = Executors.newFixedThreadPool(noOfMapThreads);
		reduceThreadPool = Executors.newFixedThreadPool(noOfReduceThreads); 
	}
	
	void config(String configFilePath)
	{
		taskTrackerId= 10;
		noOfMapThreads=5;
		noOfReduceThreads=5;
		JobTrackerIp="127.0.0.1";
		JobTrackerPort=2000;
		noOfMapThreadsRemain =noOfMapThreads;
		noOfReduceThreadsRemain =noOfReduceThreads;
		heartBeatRate = 10000;
	}
	
	byte [] generateHeartBeat()
	{
		/*	HeartBeatRequest request = new HeartBeatRequest();
			request.taskTrackerId = taskTrackerId;
			request.numMapSlotsFree = noOfMapThreadsRemain;
			request.numReduceSlotsFree = noOfReduceThreadsRemain;
		*/
			ArrayList<MapTaskStatus> mapTaskStatus = new ArrayList<MapTaskStatus>();
			ArrayList<ReduceTaskStatus> reduceTaskStatus = new ArrayList<ReduceTaskStatus>();
		
			for(MapTaskInfo key : mapTracker.keySet())
			{
				MapTaskStatus mts = new MapTaskStatus();
				Future<?> future = mapTracker.get(key);
				
				mts.jobId=key.jobId;
				mts.taskId=key.taskId;
				mts.mapOutputFile=new String("job_"+mts.jobId + "_map_" + mts.taskId);
				mts.taskCompleted=future.isDone();
				mapTaskStatus.add(mts);		
				
				// If job completed remove entry
				if(mts.taskCompleted)
				{
					mapTracker.remove(key);
					noOfMapThreadsRemain++;
				}
			}
			for(ReducerTaskInfo key : reduceTracker.keySet())
			{
				ReduceTaskStatus rts = new ReduceTaskStatus();
				rts.jobId=key.jobId;
				rts.taskId=key.taskId;
				
				Future<?> future = reduceTracker.get(key);
				rts.taskCompleted = future.isDone();
				
				reduceTaskStatus.add(rts);
				
				// If job completed remove entry
				if(rts.taskCompleted){
					reduceTracker.remove(key);
					noOfReduceThreadsRemain++;
				}
			}
			HeartBeatRequest request = new HeartBeatRequest(taskTrackerId,noOfMapThreadsRemain,noOfReduceThreadsRemain,mapTaskStatus,reduceTaskStatus);
			request.mapStatus=mapTaskStatus;
			request.reduceStatus=reduceTaskStatus;
			request.mapStatus=mapTaskStatus;
			request.reduceStatus=reduceTaskStatus;
			
		return request.toProto();
	}
	
	public void handleHeartBeatResponse(byte [] hbr)
	{
		HeartBeatResponse heartBeatResponse = new HeartBeatResponse(hbr);
		ArrayList<MapTaskInfo> mapTasks = heartBeatResponse.mapTasks;
		ArrayList<ReducerTaskInfo> reduceTasks = heartBeatResponse.reduceTasks;

		for(MapTaskInfo mti : mapTasks)
		{
			noOfMapThreadsRemain--;
			MapTask mapTask = new MapTask(mti);
			Future<?> future = mapThreadPool.submit(mapTask);
			mapTracker.put(mti, future);
		}
		
		for(ReducerTaskInfo rti : reduceTasks)
		{
			noOfReduceThreadsRemain--;
			ReduceTask reduceTask = new ReduceTask(rti);
			Future<?> future = reduceThreadPool.submit(reduceTask);
			reduceTracker.put(rti, future);
		}
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		IJobTracker iJobTracker = null;
		Registry myreg;
		try
		{
			myreg = LocateRegistry.getRegistry(JobTrackerIp,JobTrackerPort);
			iJobTracker =(IJobTracker)myreg.lookup("JobTracker");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		byte [] heartBeatRequest = generateHeartBeat();
		byte[] heartBeatResponse = null;
		try {
			heartBeatResponse = iJobTracker.heartBeat(heartBeatRequest);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		handleHeartBeatResponse(heartBeatResponse);
	}
	
	
	public static void main(String []args){
		//TODO read the conf file Create the thread pool for number of Map and reduce tasks
		//Get the IP and port of JobTracker
		String configFilePath="";
		TimerTask taskTracker = new TaskTracker(configFilePath);
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(taskTracker, 0, heartBeatRate);
	}
}
	
	class MapTask implements Runnable
	{
		int jobId,taskId;
		String inputFile,mapTaskName,mapOutputFile;
		int bolckNums;
		
		public MapTask(MapTaskInfo mapTaskInfo)
		{
			jobId = mapTaskInfo.jobId;
			taskId = mapTaskInfo.taskId;
			inputFile = Integer.toString(mapTaskInfo.inputBlocks);
			mapTaskName = mapTaskInfo.mapName;
			mapOutputFile = new String("Job"+Integer.toString(jobId)+"_"+"Task"+Integer.toString(taskId));
		}
		public void run()
		{
			try {
				Class<?> obj = Class.forName(mapTaskName).getClass();
				Method method = obj.getMethod("map", (Class<?>)null);
				
			BufferedReader buffer = new BufferedReader(new FileReader(inputFile));
			BufferedWriter writer = new BufferedWriter(new FileWriter(mapOutputFile));
			while(true)
			{
				String line = buffer.readLine();
				if(line==null)
				{
					buffer.close();
					break;
				}
				String output = "";
				
				output = method.invoke(obj, line).toString();
				writer.write(output+"\n");
			}
			writer.flush();
			writer.close();
			/*
			TODO HDFSPackage.GeneralClient genClient = new HDFSPackage.GeneralClient();
			genClient.write(mapOutputFile, mapOutputFile);*/
			
			} catch (ClassNotFoundException | IOException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e ) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			
	}
	
	class ReduceTask implements Runnable
	{
		
		public int jobId,taskId;
		public String reducerName,outputFile;
		ArrayList<String> mapOutputFiles;
		
		public ReduceTask(ReducerTaskInfo reducerTaskInfo)
		{
			jobId = reducerTaskInfo.jobId;
			taskId = reducerTaskInfo.taskId;
			reducerName = reducerTaskInfo.reducerName;
			outputFile = reducerTaskInfo.outputFile;
			mapOutputFiles = new ArrayList<String>();
			for (String s : reducerTaskInfo.mapOutputFiles)
			{
				mapOutputFiles.add(s);
			}
		}
		public void run()
		{
			Class<?> obj = null;
			try {
				obj = Class.forName(reducerName).getClass();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Method method = null;
			try {
				 method = obj.getMethod("reduce", (Class<?>)null);
			} catch (NoSuchMethodException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {			
				outputFile=outputFile+"_"+Integer.toString(jobId)+"_"+Integer.toString(taskId);
				BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
				for (String inputFile : mapOutputFiles)
				{
						/*TODO HDFSPackage.GeneralClient generalClient = new HDFSPackage.GeneralClient();
						generalClient.read(inputFile);*/
						
						BufferedReader buffer = new BufferedReader(new FileReader(inputFile));
						while(true)
						{
							String line = buffer.readLine();
							if(line==null)
							{
								buffer.close();
								break;
							}
							String output = null;
							output = method.invoke(obj, line).toString();
							if(output!=null)
								writer.write(output+"\n");
						}
						writer.flush();
				}
				writer.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}