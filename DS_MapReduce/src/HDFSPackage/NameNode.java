package HDFSPackage;
import java.util.HashMap;
import java.util.ArrayList;
import HDFSPackage.RequestResponse.*;


public class NameNode implements INameNode{
	/*Store the fileName and corresponding list of blocks*/
	HashMap<String,ArrayList<Integer> > fileNameToBlockNum = new HashMap<String,ArrayList<Integer>>(); 
	HashMap<Integer, String> fileHandleToFileName = new HashMap<Integer, String>();
	static int fileHande = 0;
	@Override
	public byte[] openFile(byte[] input) {
		// TODO Auto-generated method stub
		OpenFileRequest openFileRequest = new OpenFileRequest(input);
		OpenFileRespose openFileResponse = new OpenFileRespose();
		if(openFileRequest.forRead){/* read request for File(check proto file for more details)*/
			if(fileNameToBlockNum.containsKey(openFileRequest.fileName)){
			/*File exists write operations done and file contains some blocks*/
				fileHande++;
				openFileResponse.status = 1;//read success
				openFileResponse.handle = fileHande; //used to close the file;
				openFileResponse.blockNums = (ArrayList<Integer>)fileNameToBlockNum.get(openFileRequest.fileName);
			}
			else{/*File does not exist*/
				openFileResponse.status = -1; //file does not exist error in opening file
				openFileResponse.handle = -1;
				openFileResponse.blockNums.add(-1);
			}
		}
		else{/* write request for File(check proto file for more details)*/
			fileHande++;
			fileHandleToFileName.put(fileHande, openFileRequest.fileName);
			openFileResponse.handle = fileHande;
			/*Write operation NameNode will */
			if(fileNameToBlockNum.containsKey(openFileRequest.fileName)){
				openFileResponse.status = 1;
				openFileResponse.blockNums = (ArrayList<Integer>)fileNameToBlockNum.get(openFileRequest.fileName);
			}
			else{
				ArrayList<Integer> blocks = new ArrayList<Integer>();
				fileNameToBlockNum.put(openFileRequest.fileName, blocks);
				openFileResponse.status = 2;
				openFileResponse.blockNums.add(-1);
			}
		}
		return openFileResponse.toProto();
	}

	@Override
	public byte[] closeFile(byte[] fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBlockLocations(byte[] blockNumbers) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] assignBlock(byte[] blockLocations) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] list(byte[] fileNames) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] blockReport(byte[] blockStatus) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] heartBeat(byte[] hearBeat) {
		// TODO Auto-generated method stub
		return null;
	}

	public static void main(String[] args){
		System.out.println("Hello");
	}
}
