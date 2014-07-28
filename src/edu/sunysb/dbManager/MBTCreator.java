package edu.sunysb.dbManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MBTCreator extends Thread {
	private final int NUM_RETRY = 10;
	private final boolean UDBG = false;
	public int branchingFactor=25;
	public int height=4; 
	public static String rootHash="fe9280af7e6e06040f718a11085d1b507127559c";//"99e5b1c82c132b980c72bd9fe1ee180b0859327b";//"fc71b32e457746969cd784f33d57e01ef3e9dcd0";
	public int valueSize=50;
	public char charToFill='c';
	public int numKeys;
	public int threadId;
	public int numRuns;
	public int range;
	public int threadLow;
	public int threadHigh;
	public String newValueToUpdate;
	//public static final int numRuns=10000;
	BufferedWriter bw;
	CallableStatement stmt=null;
	Connection connection=null;
	int totalThreads;
	public int invalidCount;
	//int numTimes=4;
	public static float avgUpdatesPerSecond;
	
	/*public static void main(String args[]) throws IOException{
		MBTCreator mbtCreator= new MBTCreator();
		int numLeaves=(int) Math.pow(mbtCreator.branchingFactor,mbtCreator.height);
		mbtCreator.numKeys=numLeaves*(mbtCreator.branchingFactor-1);
		System.out.println("Number of leaves="+numLeaves);
		System.out.println("Number of keys="+mbtCreator.numKeys);
		
//		try {
//			File file = new File("btree.txt");
//			FileWriter fw = new FileWriter(file.getAbsoluteFile());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		//long startTime=System.currentTimeMillis();
		//BTreeManager bTreeManager=new BTreeManager(dbManager,numLeaves*(mbtCreator.branchingFactor-1));
		//bTreeManager.emptyDataTable();
		//String fixedStringVal=bTreeManager.fillString(valueSize, charToFill);
		//bTreeManager.populateBTreeLeaves(mbtCreator.height,mbtCreator.branchingFactor,fixedStringVal);
		//bTreeManager.populateBTreeInternalNodes(mbtCreator.height,mbtCreator.branchingFactor);
		//long endTime=System.currentTimeMillis();
		//long diff=TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
		//System.out.println("Time for Constructing BTree= "+diff+" seconds");
		
		
//		long startTime=System.currentTimeMillis();
//		MBTreeManager mbTreeManager=new MBTreeManager(dbManager,mbtCreator.branchingFactor,mbtCreator.height);
//		mbTreeManager.emptyMBTreeTable();
//		mbTreeManager.populateMBTreeLeaves(mbtCreator.height);
//		mbTreeManager.loadDataIntoTable(mbtCreator.height);
//		mbTreeManager.populateMBTreeInternalNodes(mbtCreator.height,mbtCreator.branchingFactor);	
//		dbManager.closeConnection();
//		long endTime=System.currentTimeMillis();
//		long diff=TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
//		System.out.println("Time for Constructing MBTree= "+diff+" seconds");
		int numThreads=0;
		for(int i=10000;i<=10000;i=i*10){
			int numIters=10000;
			mbtCreator.initRuns(++numThreads, numIters, i );
		}
		
	}*/
	
	public void run() {

		DBManager dbManager= new DBManager();
		FileWriter fw;
		try {
			if(UDBG){
				fw = new FileWriter("results"+threadId+".txt");
				bw=new BufferedWriter(fw);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			//DBManager.openConnection();
			connection=dbManager.openConnection();
			connection.setAutoCommit(false);
			String searchString = "{call search(?,?,?,?)}";
			stmt=connection.prepareCall(searchString);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long startTime=System.currentTimeMillis();
		
		try {
			performRunsForUpdate(threadId, numRuns, range, threadLow, threadHigh);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//mbtCreator.update(755363,756545,"ssss");
		//mbtCreator.update(755363,756545,"jjsssj");
		//mbtCreator.update(855363,855567,"hhhhh");
		dbManager.closeConnection();
		//test case for mbtree left boundary fix
		//mbtCreator.search(6120001,6161691);
		//test case for mbtree right boundary fix
		//mbtCreator.search(6120002,6135000);
		
		//test case for mbtree random left boundary fix
		//mbtCreator.search(47648849, 47668849);
		//test case for mbtreerandom right boundary fix
		//mbtCreator.search(47745926,47765926);
		
		long endTime=System.currentTimeMillis();
		long diff = endTime-startTime;
		//System.out.println("Total time for "+numRuns+" runs with range "+range+"= "+diff+" milliseconds");
		try {
			if(UDBG){
			bw.write("Total time for "+numRuns+" runs with range "+range+" = "+diff+ " milliseconds\n");
			bw.write("Total time for "+numRuns+" runs with range "+range+" = "+TimeUnit.MILLISECONDS.toSeconds(endTime-startTime)+" seconds\n");
			bw.write("Total time for "+numRuns+" runs with range "+range+" = "+TimeUnit.MILLISECONDS.toMinutes(endTime-startTime)+" minutes\n");
			bw.close();
			System.out.println("Thread "+threadId+" completed with "+numRuns+" runs. Time Taken= "+TimeUnit.MILLISECONDS.toSeconds(endTime-startTime)+" seconds. Invalid Count="+invalidCount);
			}
			
			//float numUpdatesPerSecond=((float)numRuns/(float)diff)*1000;
			
			//logSummaryResults(threadId, numRuns, range,diff, totalThreads,numUpdatesPerSecond);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
		
	}
	
	public synchronized void logSummaryResults(int threadId, int numRuns, int range, long diff, int totalThreads, float numUpdatesPerSecond){
		
		avgUpdatesPerSecond+=numUpdatesPerSecond;
		BufferedWriter cbw;
		try {
			cbw = new BufferedWriter(new FileWriter("summary-"+range+"-"+totalThreads+".csv",true));
			cbw.write(threadId+","+range+","+numRuns+","+diff+","+numUpdatesPerSecond+"\n");
			cbw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*
	public void performRunsForSearch(int numRuns, int range) throws IOException{
		if(UDBG){
		System.out.println(range);
		}
		//int leftKey[]={3757, 123656, 8934678, 3789321, 234121, 7780023 };
		//int rightKey[]={4969, 123755, 8935677, 3799320, 334120, 9780022};
		
		Random rand=new Random();
		int low=2;
		//int low=4;
		//int firstKeyHigh=72983739;
		//int secondKeyHigh=72983754;
		int firstKeyHigh=numKeys-2;
		int secondKeyHigh=numKeys-1;
		int leftKey;
		int rightKey;
		HashSet hashSet=new HashSet();
		for(int i=0;i<numRuns;i++){
			do{
				leftKey=randInt(low,firstKeyHigh);
				rightKey=randInt(leftKey+1,secondKeyHigh);
			}while((rightKey-leftKey)>range);//|| hashSet.contains(leftKey) || hashSet.contains(rightKey));
			hashSet.add(leftKey);
			hashSet.add(rightKey);
			Calendar cal = Calendar.getInstance();
			long startTime=System.currentTimeMillis();
			if(UDBG){
			System.out.println("Iteration "+i+": "+"call search("+leftKey+','+rightKey+','+branchingFactor+','+height+')');
			}
			bw.write("Iteration "+i+": "+"call search("+leftKey+','+rightKey+','+branchingFactor+','+height+')'+"\n");
			
			search(leftKey,rightKey);
			long endTime=System.currentTimeMillis();
			long diff=endTime-startTime;//TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
			
			//System.out.println("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds");
			//bw.write("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds\n");
			if(UDBG){
			System.out.println();
			}
			//bw.write("\n");
		}
	}*/
	
	public void performRunsForUpdate(int threadId, int numRuns, int range, int threadLow, int threadHigh) throws IOException{
		
		
		Random rand=new Random();
		//int low=2;
		//int firstKeyHigh=numKeys-2;
		//int secondKeyHigh=numKeys-1;
		
		//int low=4;
		//int firstKeyHigh=72983739;
		//int secondKeyHigh=72983754;
		
		int low=threadLow;
		int firstKeyHigh=threadHigh;
		int secondKeyHigh=threadHigh;
		
		
		int leftKey;
		int rightKey;
		HashSet hashSet=new HashSet();
		for(int i=0;i<numRuns;i++){
			do{
				
				leftKey=randInt(low,firstKeyHigh);
				rightKey=randInt(leftKey+1,secondKeyHigh);
				//if((rightKey-leftKey)>range || leftKey==-1 || rightKey==-1)
					//System.out.println("Regenerating "+leftKey+" "+rightKey);
			}while((rightKey-leftKey)>range || leftKey==-1 || rightKey==-1);//|| hashSet.contains(leftKey) || hashSet.contains(rightKey));
			hashSet.add(leftKey);
			hashSet.add(rightKey);
			Calendar cal = Calendar.getInstance();
			long startTime=System.currentTimeMillis();
			if(UDBG){
			System.out.println("Iteration "+i+": "+"call btreeUpdate("+leftKey+','+rightKey+','+branchingFactor+','+height+')');
			bw.write("Iteration "+i+": "+"call btreeUpdate("+leftKey+','+rightKey+','+branchingFactor+','+height+')'+"\n");
			}
			
			
			update(bw, threadId, leftKey, rightKey, newValueToUpdate);
			//update(bw, threadId, 4699133, 4699377, "bbbbbbbbb");
			try {
				//synchronized(this){
				connection.commit();
				//}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long endTime=System.currentTimeMillis();
			long diff=endTime-startTime;//TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
			
			//System.out.println("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds");
			//bw.write("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds\n");
			
			//System.out.println();
			//bw.write("\n");
		}
	}
	
	
	public int randInt(int min, int max) {

	    // Usually this can be a field rather than a method variable
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum=-1;
	    try{
	    randomNum = rand.nextInt((max - min) + 1) + min;
	    }catch(IllegalArgumentException e){
	    	//e.printStackTrace();
	    	if(UDBG){
	    	System.out.println(min+" "+max);
	    	}
	    	return randomNum;
	    }
	    return randomNum;
	}
	
	/*
	public void search(int leftKey,int rightKey) throws IOException{
		
		MBTreeSearch mbTreeSearch=new MBTreeSearch(branchingFactor,height);
		mbTreeSearch.search(connection, stmt, leftKey, rightKey);
		
		if(MBTCreator.rootHash!=null && MBTreeSearch.rootHash!=null){
			if(MBTCreator.rootHash.equals(MBTreeSearch.rootHash)){
				//System.out.println("Root Hashes Match");
				//bw.write("Root Hashes Match\n");
			}else{	
				System.out.println("Mismatching root hashes");
				bw.write("Mismatching root hashes\n");
				System.out.println("Obtained Root Hash="+ MBTreeSearch.rootHash);
				System.out.println("Actual Root Hash="+ MBTCreator.rootHash);		
			}
		}else{
			bw.write("One of the root hash is null\n");
			System.out.println("One of the root hash is null");
			System.out.println("Obtained root hash="+ MBTreeSearch.rootHash);
		}
		
		
		
	}*/
	
public void update(BufferedWriter bw, int threadId, int leftKey,int rightKey, String newVal) throws IOException{
		
		int count=0;
		int retVal;
		do{
			if(count==NUM_RETRY)
				break;
			if(UDBG){
			System.out.println("Trying for "+leftKey+" "+rightKey);
			}
			MBTreeUpdate mbTreeUpdate=new MBTreeUpdate(branchingFactor, height);
			retVal=mbTreeUpdate.update(bw, threadId, stmt, leftKey, rightKey, newVal);
			if(retVal==-1)
				invalidCount++;
			count++;
			
		}while(retVal==-3);
		
	
		
	}

public static void main(String args[]) throws IOException, SQLException{
	MBTCreator mbtCreator=new MBTCreator();
	
	int leftKey=9613914;
	int rightKey=9616842;
	int threadId=1;
	FileWriter fw = new FileWriter("results");
	mbtCreator.bw=new BufferedWriter(fw);
	
	DBManager dbManager=new DBManager();
	mbtCreator.connection=dbManager.openConnection();
	//mbtCreator.connection=dbManager.getConnection();
	mbtCreator.connection.setAutoCommit(false);
	String searchString = "{call search(?,?,?,?)}";
	mbtCreator.stmt=mbtCreator.connection.prepareCall(searchString);
	int retVal=-3;
	do{
		MBTreeUpdate mbTreeUpdate=new MBTreeUpdate(mbtCreator.branchingFactor, mbtCreator.height);
		retVal=mbTreeUpdate.update(mbtCreator.bw, 1, mbtCreator.stmt, 4890863,4891528, "bbbbbb");
	}while(true);
}

}
