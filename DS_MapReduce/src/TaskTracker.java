import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import MapReducePkg.MRRequestResponse.MapTaskInfo;
import MapReducePkg.MRRequestResponse.ReducerTaskInfo;

public class TaskTracker{
	// TODO When the TT gets a map request, it places the request in an internal
//	queue. The consumers of this queue are a fixed number of threads
	//TODO  Threadpool creation "http://tutorials.jenkov.com/java-concurrency/thread-pools.html"
	//TODO take input from conf file 
	//TaskTracker ID,Jobtracker Ip,Port,Number of Map threads,Number of reduce threads
	
	public static void main(String []args){
		//TODO read the conf file Create the thread pool for number of Map and reduce tasks
		//Get the IP and port of JobTracker
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
}
