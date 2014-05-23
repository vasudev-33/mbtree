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

public class MBTCreator {
	public static int branchingFactor=25;
	public static int height=4; 
	public static String rootHash="fe46c754c9a3a11b379ec4f95ad931226b1b5a58";//"99e5b1c82c132b980c72bd9fe1ee180b0859327b";//"fc71b32e457746969cd784f33d57e01ef3e9dcd0";
	public static int valueSize=50;
	public static char charToFill='a';
	public static int numKeys;
	public static final int numRuns=10000;
	static BufferedWriter bw;
	static CallableStatement stmt=null;
	static Connection connection=null;
	int numTimes=4;
	
	public static void main(String args[]) throws IOException{
		MBTCreator mbtCreator= new MBTCreator();
		DBManager dbManager= new DBManager();
		
		int numLeaves=(int) Math.pow(MBTCreator.branchingFactor,MBTCreator.height);
		numKeys=numLeaves*(MBTCreator.branchingFactor-1);
		System.out.println("Number of leaves="+numLeaves);
		//bw.write("Number of leaves="+numLeaves+'\n');
		System.out.println("Number of keys="+numKeys);
		//bw.write("Number of keys="+numKeys+"\n\n");
		
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
		for(int i=10;i<=10000;i=i*10){
			
			FileWriter fw;
			try {
				fw = new FileWriter("results"+i+".txt");
				bw=new BufferedWriter(fw);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				DBManager.openConnection();
				connection=dbManager.getConnection();
				String searchString = "{call search(?,?,?,?)}";
				stmt=connection.prepareCall(searchString);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long startTime=System.currentTimeMillis();
			mbtCreator.performRunsForUpdate(numRuns,i);
			//mbtCreator.verifyAndUpdate(655363,655371);
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
			System.out.println("Total time for "+numRuns+" runs with range "+i+"= "+diff+" milliseconds");
			bw.write("Total time for "+numRuns+" runs with range "+i+" = "+diff+ " milliseconds\n");
			bw.write("Total time for "+numRuns+" runs with range "+i+" = "+TimeUnit.MILLISECONDS.toSeconds(endTime-startTime)+" seconds\n");
			bw.write("Total time for "+numRuns+" runs with range "+i+" = "+TimeUnit.MILLISECONDS.toMinutes(endTime-startTime)+" minutes\n");
			bw.close();
		}
		
	}
	
	public void performRunsForSearch(int numRuns, int range) throws IOException{
		System.out.println(range);
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
			
			System.out.println("Iteration "+i+": "+"call search("+leftKey+','+rightKey+','+MBTCreator.branchingFactor+','+MBTCreator.height+')');
			bw.write("Iteration "+i+": "+"call search("+leftKey+','+rightKey+','+MBTCreator.branchingFactor+','+MBTCreator.height+')'+"\n");
			
			search(leftKey,rightKey);
			long endTime=System.currentTimeMillis();
			long diff=endTime-startTime;//TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
			
			//System.out.println("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds");
			//bw.write("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds\n");
			
			System.out.println();
			//bw.write("\n");
		}
	}
	
	public void performRunsForUpdate(int numRuns, int range) throws IOException{
		//System.out.println(range);
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
			
			System.out.println("Iteration "+i+": "+"call btreeUpdate("+leftKey+','+rightKey+','+MBTCreator.branchingFactor+','+MBTCreator.height+')');
			bw.write("Iteration "+i+": "+"call btreeUpdate("+leftKey+','+rightKey+','+MBTCreator.branchingFactor+','+MBTCreator.height+')'+"\n");
			
			verifyAndUpdate(leftKey, rightKey);
			long endTime=System.currentTimeMillis();
			long diff=endTime-startTime;//TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
			
			//System.out.println("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds");
			//bw.write("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds\n");
			
			System.out.println();
			//bw.write("\n");
		}
	}
	
	public void verifyAndUpdate(int leftKey,int rightKey) throws IOException{
		//search(leftKey, rightKey);
		update(leftKey, rightKey, "bbbbbbbbb");
		
		
	}
	
	public int randInt(int min, int max) {

	    // Usually this can be a field rather than a method variable
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min) + 1) + min;

	    return randomNum;
	}
	
	public void search(int leftKey,int rightKey) throws IOException{
		
		MBTreeSearch mbTreeSearch=new MBTreeSearch();
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
		
		
		
	}
	
public void update(int leftKey,int rightKey, String newVal) throws IOException{
		
		MBTreeUpdate mbTreeUpdate=new MBTreeUpdate();
		mbTreeUpdate.update(connection, stmt, leftKey, rightKey, newVal);
		
		
	}

}
