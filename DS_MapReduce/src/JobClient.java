import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.ExecutionException;

import MapReducePkg.MRRequestResponse.JobStatusRequest;
import MapReducePkg.MRRequestResponse.*;


public class JobClient {
	public static String jobTrackerIP="127.0.0.1";
	public static int jobTrackerPort=2000;
	int createJob(String mapName,String reduceName,String inFile,String outFile,int numOfReducers){
		IJobTracker in = null;
		JobSubmitResponse jobSubmitResponse = null;
		byte []jobSubmitResponseData = null;
		try {
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
					Thread.sleep(1000);
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
		if(args.length!=5){
			System.out.println("Usage <mapName> <reducerName> <inputFile in HDFS> <outputFile in HDFS> <numReducers> JobClient finds");
			System.exit(0);
		}
//		TODO conf file for finding location of JobTracker
		String m = args[0],r = args[1],i = args[2],o=args[3];
		int  n = Integer.parseInt(args[4]);
		JobClient jBClient = new JobClient();
		jBClient.createJob(m,r,i,o,n);
	}
}
