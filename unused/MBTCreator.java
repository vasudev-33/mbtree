package edu.sunysb.dbManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.swing.text.AbstractDocument.BranchElement;

public class MBTCreator {
	public static int branchingFactor=25;
	public static int height=4; 
	public static String rootHash;
	public static int valueSize=50;
	public static char charToFill='a';
	
	
	public static void main(String args[]){
		MBTCreator mbtCreator=new MBTCreator();
		DBManager dbManager= new DBManager();
		
		int numLeaves=(int) Math.pow(mbtCreator.branchingFactor,mbtCreator.height);
		System.out.println("Number of leaves="+numLeaves);
		
		
//		try {
//			File file = new File("btree.txt");
//			FileWriter fw = new FileWriter(file.getAbsoluteFile());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		BTreeManager bTreeManager=new BTreeManager(dbManager,numLeaves*(mbtCreator.branchingFactor-1));
		//bTreeManager.emptyDataTable();
		//String fixedStringVal=bTreeManager.fillString(valueSize, charToFill);
		//bTreeManager.populateBTreeLeaves(mbtCreator.height,mbtCreator.branchingFactor,fixedStringVal);
//		bTreeManager.populateBTreeInternalNodes(mbtCreator.height,mbtCreator.branchingFactor);
//		
//		
		MBTreeManager mbTreeManager=new MBTreeManager(dbManager,mbtCreator.branchingFactor,mbtCreator.height);
		mbTreeManager.emptyMBTreeTable();
		mbTreeManager.populateMBTreeLeaves(mbtCreator.height);
//		mbTreeManager.populateMBTreeInternalNodes(mbtCreator.height,mbtCreator.branchingFactor);
//		
//		dbManager.closeConnection();
		
//		String searchQuery="select ";
//		dbManager =new DBManager();
//		dbManager.openConnection();
//		Connection connection=dbManager.getConnection();
//		MBTreeSearch mbTreeSearch=new MBTreeSearch();
//		mbTreeSearch.search(connection, searchQuery);
//		dbManager.closeConnection();
//		if(MBTCreator.rootHash!=null && MBTreeSearch.rootHash!=null){
//			if(MBTCreator.rootHash.equals(MBTreeSearch.rootHash)){
//				System.out.println("Root Hashes Match");
//			}else{
//				System.out.println("Mismatching root hashes");
//			}
//		}else{
//			System.out.println("One of the root hash is null");
//			System.out.println("Obtained root hash="+MBTreeSearch.rootHash);
//		}
		
		
	}

}
