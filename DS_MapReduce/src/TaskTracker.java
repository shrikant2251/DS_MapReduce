
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import HDFSPackage.GeneralClient;
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
	static String genClientConf = new String();
	static ConcurrentHashMap<MapTaskInfo,Future<?>> mapTracker ;
	static ConcurrentHashMap<ReducerTaskInfo,Future<?>> reduceTracker;
	static ExecutorService mapThreadPool,reduceThreadPool;
	static String tmpMapDir,tmpReduceDir,blockDir;
	
	// TODO When the TT gets a map request, it places the request in an internal
//	queue. The consumers of this queue are a fixed number of threads
	//TODO  Threadpool creation "http://tutorials.jenkov.com/java-concurrency/thread-pools.html"
	//TODO take input from conf file 
	//TaskTracker ID,Jobtracker Ip,Port,Number of Map threads,Number of reduce threads

	public TaskTracker(String configFilePath)
	{
		super();
		config(configFilePath);
		mapTracker = new ConcurrentHashMap<MapTaskInfo,Future<?>>();
		reduceTracker = new ConcurrentHashMap<ReducerTaskInfo,Future<?>>();
		mapThreadPool = Executors.newFixedThreadPool(noOfMapThreads);
		reduceThreadPool = Executors.newFixedThreadPool(noOfReduceThreads); 
	}
	
	void config(String configFilePath)
	{
		try{
			File f = new File(configFilePath);
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = null;
			while((line=br.readLine())!=null){
				String []data = line.split("=");
				switch(data[0]){
					case "taskTrackerId":taskTrackerId = Integer.parseInt(data[1]);break;
					case "noOfMapThreads":noOfMapThreads = Integer.parseInt(data[1]);noOfMapThreadsRemain=noOfMapThreads;break;
					case "noOfReduceThreads":noOfReduceThreads = Integer.parseInt(data[1]);noOfReduceThreadsRemain=noOfReduceThreads;break;
					case "JobTrackerIp":JobTrackerIp = data[1];break;
					case "JobTrackerPort": JobTrackerPort=Integer.parseInt(data[1]);break;
					case "heartBeatRate":heartBeatRate = Integer.parseInt(data[1]);break;
					case "genClientConf":genClientConf=data[1];break;
					case "tmpMapDir":tmpMapDir=data[1];break;
					case "tmpReduceDir":tmpReduceDir=data[1];break;
					case "blockDir":blockDir=data[1];break;
				}
			}
			br.close();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
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
					System.out.println("TaskTracker Map Completed Jobid:" + mts.jobId + " TaskId:" + mts.taskId);
					System.out.println("No of Mapthreads free:" + noOfMapThreadsRemain );
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
		System.out.println("AAAAAAAAAAAAA : TaskTracker HandleHeartBeat :" + reduceTasks.size());
		for(ReducerTaskInfo rti : reduceTasks)
		{
			for(String m:rti.mapOutputFiles){
				System.out.println("++++++++++++++++++++++++++++++++++++++++ TaskTrcker Handle heartBeat MapOutpufileName :" + m);
			}
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
		if(args.length!=1){
			System.out.println("Invalid Usage");
			System.out.println("Usage java <TaskTracker> <configFile Path>");
			System.exit(0);
		}
		String configFilePath=args[0];
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
		//TODO update the names
		String directoryName = TaskTracker.blockDir;
		String tmpDir = TaskTracker.tmpMapDir;
		public MapTask(MapTaskInfo mapTaskInfo)
		{
			jobId = mapTaskInfo.jobId;
			taskId = mapTaskInfo.taskId;
			inputFile = Integer.toString(mapTaskInfo.inputBlocks);
			mapTaskName = mapTaskInfo.mapName;
			mapOutputFile = new String("Job_"+Integer.toString(jobId)+"_map_"+Integer.toString(taskId));
		}
		public void run()
		{
			try {
				File dir = new File(tmpDir);
				if (!dir.exists()) {
					dir.mkdirs();
				}
				System.out.println("TaskTracker MapTask Class run method mapName : " + mapTaskName);
				System.out.println("Maptask run method Input File :" +directoryName + inputFile);
				System.out.println("MapTask run method OutputFile :" +tmpDir+mapOutputFile);
				Class obj = Class.forName(mapTaskName);
				System.out.println("TaskTracker Maptask run method Object bound to " + obj.getName());
				//Method method = obj.getMethod("map", new Class[]{String.class});
				IMapper mapObj = (IMapper) obj.newInstance();
				BufferedReader buffer = new BufferedReader(new FileReader(directoryName+inputFile));
				BufferedWriter writer = new BufferedWriter(new FileWriter(tmpDir+mapOutputFile));
				while(true)
				{
					String line = buffer.readLine();
					if(line==null)
					{
						buffer.close();
						break;
					}
					String output = "";
					
					output = mapObj.map(line);
					if(output!=null)
					writer.write(output+"\n");
				}
				writer.flush();
				writer.close();
			
		//	TODO HDFSPackage.GeneralClient genClient = new HDFSPackage.GeneralClient();
				//copyToHDFS
				GeneralClient genClient = new GeneralClient(TaskTracker.genClientConf);
			genClient.write(mapOutputFile,tmpDir + mapOutputFile);
			
			} catch ( NotBoundException | ClassNotFoundException | IOException  | SecurityException | IllegalAccessException | IllegalArgumentException e ) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
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
		String directoryName = TaskTracker.blockDir;
		String tmpDir = TaskTracker.tmpReduceDir;
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
				obj = Class.forName(reducerName);
				System.out.println("TaskTracker ReduceTask reduceName :" + reducerName);
				//Method method = obj.getMethod("reduce", (Class<?>)null);
				IReducer reduceObj = (IReducer)obj.newInstance();
				//outputFile=outputFile+"_Reduce_"+Integer.toString(jobId)+"_"+Integer.toString(taskId);
				File tmp = new File(tmpDir);
				if(!tmp.exists()){
					tmp.mkdirs();
				}
				BufferedWriter writer = new BufferedWriter(new FileWriter(tmpDir+outputFile));
				GeneralClient genClient = new GeneralClient(TaskTracker.genClientConf);
				
				String tmpFile = tmpDir+outputFile+"tmp";
				int tmpCnt = 0;//mapOutputFiles.size();
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%  MapOutPutFile size :" + mapOutputFiles.size());
				for (String inputFile : mapOutputFiles)
				{
						/*TODO HDFSPackage.GeneralClient generalClient = new HDFSPackage.GeneralClient();
						generalClient.read(inputFile);*/
					//TODO copyFromHDFS
					System.out.println("TaskTracker ReduceTask run method " + outputFile);
					
					int status = genClient.read(inputFile, tmpFile+""+tmpCnt);
					System.out.println("##################### TaskTracker ReduceTask reading file from HDFS :" + inputFile + " to Local File:" + tmpFile+""+tmpCnt);
					System.out.println("############# Read Status :" + status);
					if(status == -1) continue;
					tmpCnt++;
					BufferedReader buffer = new BufferedReader(new FileReader(tmpFile+""+(tmpCnt-1)+"GenClientRead"));
					while(true)
					{
						String line = buffer.readLine();
						if(line==null)
						{
							buffer.close();
							break;
						}
						String output = null;
						output = reduceObj.reduce(line);
						System.out.println("********** Reduce Output ***********:" + output);
						if(output!=null)
							writer.write(output+"\n");
					}
					writer.flush();
				}
				writer.close();
				//TODO copyToHDFS
				/*
				BufferedWriter rw = new BufferedWriter(new FileWriter(tmpFile));
				rw.close();
				for(int i=0;i<tmpCnt;i++){
					BufferedReader rd = new BufferedReader(new FileReader(tmpFile+""+i));
					String line =null;
					while((line=rd.readLine())!=null){
						rw.write(line);
					}
					rd.close();
				}*/
				System.out.println("!!!!!!!!!!!!!!!!!!!!!!! ReduceTask Local File :" + tmpDir + outputFile);
				genClient.write(outputFile, tmpDir+outputFile);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}