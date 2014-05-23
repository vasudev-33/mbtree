package edu.sunysb.dbManager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BTreeManager {
	DBManager dbManager;
	PreparedStatement stmt;
	int numLeaves;
	String dbName="btree";
	BufferedWriter bw=null;
	boolean autoCommit=true;
	String path="D:\\\\workspaces\\\\AdvProject\\\\MBTDB\\\\btreeInternalLevel";
	String randPath="D:\\workspaces\\AdvProject\\MBTDB\\randomNumberTable\\sortedRandomNumberFile0.txt";
	String fileExtension=".txt";
	
	
	public BTreeManager(DBManager dbManager) {
		this.dbManager=dbManager;
		
	}

	public BTreeManager(DBManager dbManager, int numLeaves) {
		this.dbManager=dbManager;
		this.numLeaves=numLeaves;
		// TODO Auto-generated constructor stub
	}
	
	public void emptyDataTable(){
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

	public void populateBTreeLeaves(int height,int branchingFactor,String fixedStringVal){
		int curLeafId=0;
		int count=0;
		
		File file = new File("btreeInternalLevel"+height+".txt");
		
		 
		// if file doesnt exists, then create it
		try {
			if (!file.exists())
				file.createNewFile();
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			bw = new BufferedWriter(fw);
			
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		
		File randFile=new File(randPath);
		FileReader randFileReader;
		BufferedReader br=null;
		try {
			randFileReader = new FileReader(randFile);
			br=new BufferedReader(randFileReader);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		for(int j=1;j<=numLeaves;j++){
			//read a line from the randomDatafile
			String line=null;
			try {
				line=br.readLine();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//get the integer and assign it to i
			int i=Integer.parseInt(line);
			String s=i+":"+fixedStringVal;
			
			//Blob blob=new SerialBlob(byteArray);
			count++;
			if(count==branchingFactor){
				count=1;
				curLeafId++;
			}
			System.out.println(i);
			
			//insertDataDetailsDB(height,curLeafId,i, blob);
			insertDataDetailsDB(height,curLeafId,i, s);
			
		}
		try {
			bw.close();
			loadDataIntoTable(height);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void populateBTreeInternalNodes(int height,int branchingFactor){
		
		
		File file = new File("btreeInternalLevel"+(height-1)+".txt");
		
		 
		// if file doesnt exists, then create it
		try {
			if (!file.exists())
				file.createNewFile();
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			bw = new BufferedWriter(fw);
			
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		for(int i=height-1;i>=0;i--){
			populateBTreeTableLevel(i, branchingFactor, height);
		}
		
		try {
			bw.close();
			loadDataIntoTable(height-1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void loadDataIntoTable(int i) {
		System.out.println("Loading level "+i);
		System.out.println(path);
		
		String tempQuery=" LOAD DATA LOCAL INFILE " + "\'"+path+i+fileExtension+"\'"+
                " INTO TABLE btree " +
                " FIELDS TERMINATED BY \',\' " +
                " LINES TERMINATED BY \'\\n\'";
		//String query="INSERT INTO mbtree SELECT * FROM mbtreetemp";
		//String emptyTableTemp="delete from mbtreetemp";
		PreparedStatement stmt;
		try {
			//stmt = dbManager.getConnection().prepareStatement(emptyTableTemp);
			//stmt.executeUpdate();
			//if(autoCommit==false)
			//	dbManager.getConnection().commit();
			stmt = dbManager.getConnection().prepareStatement(tempQuery);
			stmt.executeUpdate();
			if(autoCommit==false)
				dbManager.getConnection().commit();
			//stmt=dbManager.getConnection().prepareStatement(query);
			//stmt.executeUpdate();
			//if(autoCommit==false)
			//	dbManager.getConnection().commit();
			
			System.out.println("Loading complete");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void populateBTreeTableLevel(int level,int branchingFactor, int height){
		try {
			//HashMap<Integer,ArrayList<Integer>> leafKeyMap=new HashMap<Integer,ArrayList<Integer>>();
			int curLevel=level;
			int bottomUpLevel=height-curLevel;
			int nextLevel=level+1;
			int numNodesAtLevel=(int) Math.pow(branchingFactor, curLevel);
			int constantValForLevel=(int) Math.pow(branchingFactor, bottomUpLevel-1);
			PreparedStatement selStmt=dbManager.getConnection().prepareStatement("select key_id from "+dbName+ " where level_id=? and leaf_id=? order by key_id");
			selStmt.setInt(1, height);
			//iterate for the total number of nodes at this level
			System.out.println("Nodes for level "+level);
			for(int i=0;i<numNodesAtLevel;i++){
				int curLeafId=i;
				
				System.out.println("Populating "+i+" leaf");
				//for each node there will be branchingfactor-1 items
				int leafOffset=((int)Math.pow(branchingFactor, bottomUpLevel))*curLeafId;
				System.out.println();
				for(int j=1;j<branchingFactor;j++){
					int lastLevelLeafId=leafOffset+j*constantValForLevel;
					//System.out.println("last Level leaf id is "+lastLevelLeafId);
					selStmt.setInt(1, height);
					selStmt.setInt(2, lastLevelLeafId);
					ResultSet rs=selStmt.executeQuery();
					rs.next();
					int firstElemOfLeaf=rs.getInt(1);
					//System.out.println(firstElemOfLeaf);
					insertDataDetailsDB(curLevel, curLeafId, firstElemOfLeaf, null);
				}
			}
			
			/*
			while(rs.next()){
				int keyId=rs.getInt(2);
				int leafId=rs.getInt(1);
				if(leafKeyMap.containsKey(leafId)){
					ArrayList<Integer> list=leafKeyMap.get(leafId);
					list.add(keyId);
					leafKeyMap.put(leafId, list);
				}else{
					ArrayList<Integer> list=new ArrayList<Integer>();
					list.add(keyId);
					leafKeyMap.put(leafId, list);
				}	
			}
			
			Iterator<Integer> iter=leafKeyMap.keySet().iterator();
			int count=0;
			int curLeafId=0;
			if(iter.hasNext()){
				iter.next();
			}
			while(iter.hasNext()){
				count++;
				if(count==branchingFactor){
					count=1;
					curLeafId++;
					iter.next();
				}
				
				if(iter.hasNext()){
					int key=iter.next();
					ArrayList<Integer> list=leafKeyMap.get(key);
					System.out.println(list.toString());
					int minElem=list.get(0);
					System.out.println(minElem);
					insertDataDetailsDB(level, curLeafId, minElem, null);
				}
				
			}*/

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void insertDataDetailsDB(int levelId,int leafId,int key,String value) {	
		//insertData(levelId,leafId,key,value);
		insertIntoFile(levelId,leafId,key,value);
	}

	private void insertIntoFile(int levelId, int leafId, int key, String value) {
		String content=levelId+","+leafId+","+key+","+value+"\n";
		// if file doesnt exists, then create it
		try {
			
			bw.write(content);
			
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		

		
		// TODO Auto-generated method stub
		
	}

	private void insertData(int levelId,int leafId,int key,String value) {
		try 
		{
			
			if(stmt==null){
			stmt = dbManager.getConnection().prepareStatement("insert into "+ dbName+ " values(?,?,?,?)");
			}
			stmt.setInt(1, levelId);
			stmt.setInt(2, leafId);
			stmt.setInt(3, key);
			stmt.setString(4, value);
			
			stmt.execute();
			
		} 
		catch (SQLException e2) {
			
			e2.printStackTrace();
		}
		
	}
	
	
	
//	public void updateTargetFileDetails(int tId, int lId) {
//			try 
//			{
//				
//				PreparedStatement stmt = dbManager.getConnection().prepareStatement("update target_master set license=? where tid=?;");
//				
//				stmt.setInt(1,lId);
//				stmt.setInt(2,tId);
//				stmt.execute();
//				
//			} 
//			catch (SQLException e2) 
//			{
//				
//				e2.printStackTrace();
//			}
//			
//		}
		
//	public int getStartIdForCurrentProject(){
//		int startId=0;
//		try {
//			
//			PreparedStatement stmt1 = dbManager.getConnection().prepareStatement("select max(tid) from target_master");
//			ResultSet rs=stmt1.executeQuery();
//			while(rs.next()){
//				
//				startId=rs.getInt(1);
//				
//			}
//			stmt1.close();
//		}
//			
//			catch (SQLException e2) 
//			{
//				e2.printStackTrace();
//			}
//		
//		return startId;
//	}
	
}

