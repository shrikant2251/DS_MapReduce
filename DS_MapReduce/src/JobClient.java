import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.FieldPosition;
import java.util.concurrent.ExecutionException;

import HDFSPackage.GeneralClient;
import MapReducePkg.MRRequestResponse.JobStatusRequest;
import MapReducePkg.MRRequestResponse.*;


public class JobClient {
	public static String jobTrackerIP;
	public static int jobTrackerPort;
	public static int jobStatusResponseTime = 3000;
	public static String genClientConfFile = "/home/shrikant/git/DS_MapReduce/Config/JobClientConf";
	public JobClient(String confFile) {
		// TODO Auto-generated constructor stub
		config(confFile);
	}
	void config(String confFile){
		System.out.println("Conf File update");
		try{
			String line =null;
			File file = new File(confFile);
			BufferedReader br = new BufferedReader(new FileReader(file));
			while((line=br.readLine())!=null){
				String []data = line.split("=");
				switch (data[0]) {
					case "jobTrackerIP": jobTrackerIP = data[1];System.out.println(data[1]);break;
					case "jobTrackerPort" : jobTrackerPort=Integer.parseInt(data[1]);break;
					case "jobStatusResponseTime" : jobStatusResponseTime = Integer.parseInt(data[1]);break;
					case "genClientConfFile" : genClientConfFile = data[1];System.out.println("Config Method path:" + genClientConfFile);break;
				}
			}
			br.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	int createJob(String mapName,String reduceName,String inFile,String outFile,int numOfReducers){
		IJobTracker in = null;
		JobSubmitResponse jobSubmitResponse = null;
		byte []jobSubmitResponseData = null;
		//GeneralClient genClient = new GeneralClient(genClientConfFile);
		try {
			//CopyToHDFS
			///genClient.write(inFile, inFile);
			System.out.println(jobTrackerIP + " " + jobTrackerPort);
			Registry myreg = LocateRegistry.getRegistry(jobTrackerIP,jobTrackerPort);
			in = (IJobTracker) myreg.lookup("JobTracker");
			JobSubmitRequest jobSubmitRequest = new JobSubmitRequest(mapName,reduceName,inFile,outFile,numOfReducers);
			jobSubmitResponseData = in.jobSubmit(jobSubmitRequest.toProto());
			jobSubmitResponse = new JobSubmitResponse(jobSubmitResponseData);
			System.out.println("JobClient CreateJob returned JobId :" + jobSubmitResponse.jobId);
		} 
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("JobClient createJobMethod Failure RMI connection with JobTracker");
			
		}		
		if(jobSubmitResponse.status == -1){
			System.out.println("JobClient createJob Method Failed due to JobSubmitResponse Failure");
			return -1;
		}
		else{
			while(true){
				//TODO keep delay for 3 sec
				try{
					Thread.sleep(jobStatusResponseTime);
				}
				catch(Exception e ){
					e.printStackTrace();
				}
				System.out.println("Get Status from JobTracker");
				try {
					JobStatusRequest jsStatusRequest = new JobStatusRequest(jobSubmitResponse.jobId);
					byte []jobStatusResponseData = in.getJobStatus(jsStatusRequest.toProto());
					JobStatusResponse jsResponse = new JobStatusResponse(jobStatusResponseData);
					if(jsResponse.jobDone){
						System.out.println("JobId : " + jobSubmitResponse.jobId + " 100% Completed !!");
						break;
					}
						
					else{
						//TODO print the Job Completion status 
						System.out.println("Num of Map started : " + jsResponse.numMapTasksStarted + "total Map :" + jsResponse.totalMapTasks);;
						if(jsResponse.numMapTasksStarted <= jsResponse.totalMapTasks){
							System.out.println("JobId : " + jobSubmitResponse.jobId +" -->" + (jsResponse.numMapTasksStarted / jsResponse.totalMapTasks)*100 + "% of MapTask Started");
						}
						
						if(jsResponse.numReduceTasksStarted <= jsResponse.totalReduceTasks){
							System.out.println("JobId : " + jobSubmitResponse.jobId +" -->" + (jsResponse.numReduceTasksStarted / jsResponse.totalReduceTasks)*100 + "% of ReduceTask Started");
						}

					}
				}
				catch(Exception e){
					e.printStackTrace();
					System.out.println("JobClient CreateJob Method excetion while getJobStatus");
					return -1;
				}
				//TODO Termination condition
			}
		}
		return 1 ;// success
	}
	public static void main(String []args){
		if(args.length!=6){
			System.out.println("Usage <mapName> <reducerName> <inputFile in HDFS> <outputFile in HDFS> <numReducers> <ConfFile Path>");
			System.exit(0);
		}
//		TODO conf file for finding location of JobTracker
		String m = args[0],r = args[1],i = args[2],o=args[3];
		int  n = Integer.parseInt(args[4]);
		System.out.println(args[5]);
		JobClient jBClient = new JobClient(args[5]);
		System.out.println("Main Method :" + JobClient.jobTrackerIP + " " + JobClient.jobTrackerPort);
		jBClient.createJob(m,r,i,o,n);
	}
}
