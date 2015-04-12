import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import MapReducePkg.MRRequestResponse.*;


public class JobClient {
	public static String jobTrackerIP;
	public static int jobTrackerPort;
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
				try {
					byte []jobStatusResponseData = in.getJobStatus(jobSubmitResponseData);
					JobStatusResponse jsResponse = new JobStatusResponse(jobStatusResponseData);
					if(jsResponse.jobDone){
						System.out.println("JobId : " + jobSubmitResponse.jobId + " 100% Completed !!");
						break;
					}
						
					else{
						//TODO print the Job Completion status 
						if(jsResponse.numMapTasksStarted < jsResponse.totalMapTasks){
							System.out.println("JobId : " + jobSubmitResponse.jobId +" -->" + (jsResponse.numMapTasksStarted / jsResponse.totalMapTasks) + "% of MapTask Started");
						}

						if(jsResponse.numReduceTasksStarted < jsResponse.totalReduceTasks){
							System.out.println("JobId : " + jobSubmitResponse.jobId +" -->" + (jsResponse.numReduceTasksStarted / jsResponse.totalReduceTasks) + "% of ReduceTask Started");
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
