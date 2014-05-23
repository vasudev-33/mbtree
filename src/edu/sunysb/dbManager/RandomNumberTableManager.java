package edu.sunysb.dbManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Random;

public class RandomNumberTableManager {
	DBManager dbManager;
	Connection conn;
	int totalNumbers=11000000;
	int flushSize=100000;
	boolean autoCommit=false;
	int randMin=1;
	int randMax=80000000;
	PreparedStatement stmt;
	String dbName="randomnumbertable";
	public static int valueSize=50;
	public static char charToFill='a';
	BufferedWriter bw=null;
	String path="D:\\\\workspaces\\\\AdvProject\\\\MBTDB\\\\randomNumberTable\\\\randomNumberTable";
	String randPath="D:\\workspaces\\AdvProject\\MBTDB\\randomNumberTable\\randomNumberTable";
	String fileExt=".txt";
	public static void main(String args[]){
		RandomNumberTableManager rm=new RandomNumberTableManager();
		rm.dbManager=new DBManager();
		//rm.emptyRandomTable();
		//for(int i=0;i<100;i++){
			rm.generateRandomData(0);
			//rm.loadDataIntoTable(i);
		//}
		
		rm.dbManager.closeConnection();
		
	}
	
	public void generateRandomData(int fileIndex){
		
		conn=dbManager.getConnection();
		try{
			File file = new File(randPath+fileIndex+".txt");
			if (!file.exists())
				file.createNewFile();
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			bw = new BufferedWriter(fw);
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		String fixedStringVal=fillString(valueSize, charToFill);
		try {
			conn.setAutoCommit(false);
			HashSet hashSet=new HashSet();
			for(int i=0;i<totalNumbers;i++){
				if(i%flushSize==0 && autoCommit==false){
					conn.commit();
					conn.setAutoCommit(false);
				}
				int randNum=randInt(randMin, randMax);
				if(hashSet.contains(randNum))
					continue;
				hashSet.add(randNum);
				String s=randNum+":"+fixedStringVal;
				System.out.println(i+" "+randNum+" "+s);
				insertDataDetailsDB(randNum, s);
			}
			bw.close();
			//stmt.close();
			
			conn.commit();
			conn.setAutoCommit(true);
			
			//dbManager.closeConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public int randInt(int min, int max) {

	    // Usually this can be a field rather than a method variable
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min) + 1) + min;

	    return randomNum;
	}
	
	public void insertDataDetailsDB(int key,String value) {	
		//insertData(levelId,leafId,key,value);
		//insertData(key,value);
		insertIntoFile(key, value);
	}

	private void insertData(int key,String value) {
		try 
		{
			
			if(stmt==null){
			stmt = conn.prepareStatement("insert into "+ dbName+ " values(?,?)");
			}
			
			stmt.setInt(1, key);
			stmt.setString(2, value);
			
			stmt.execute();
			
		} 
		catch (SQLException e2) {
			
			e2.printStackTrace();
		}
		
	}
	
	private void insertIntoFile( int key, String value) {
		String content=key+","+value+"\n";
		// if file doesnt exists, then create it
		try {
			
			bw.write(content);
			
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		

		
		// TODO Auto-generated method stub
		
	}
	
	public void emptyRandomTable(){
		try {
			PreparedStatement delStmt = dbManager.getConnection().prepareStatement("delete from "+dbName);
			delStmt.execute();
			delStmt.close();
		} 
		catch (SQLException e2) {
			
			e2.printStackTrace();
		}
		
	}
	
	public static String fillString(int count,char c) {
	    StringBuilder sb = new StringBuilder( count );
	    for( int i=0; i<count; i++ ) {
	        sb.append( c ); 
	    }
	    return sb.toString();
	}
	
	public void loadDataIntoTable(int fileIndex) {
		
		System.out.println("Loading "+path+fileIndex+fileExt);
		Connection conn=dbManager.getConnection();
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String tempQuery=" LOAD DATA LOCAL INFILE " + "\'"+path+fileIndex+fileExt+"\'"+
                " INTO TABLE randomnumbertable2 " +
                " FIELDS TERMINATED BY \',\' " +
                " LINES TERMINATED BY \'\\n\'";
		//String query="INSERT INTO mbtree SELECT * FROM mbtreetemp";
		//String emptyTableTemp="delete from mbtreetemp";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(tempQuery);
			stmt.executeUpdate();
			//if(autoCommit==false)
				conn.commit();
			System.out.println("Loading complete");
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
