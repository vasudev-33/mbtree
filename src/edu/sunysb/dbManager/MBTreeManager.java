package edu.sunysb.dbManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.TreeMap;

public class MBTreeManager {
	DBManager dbManager;
	PreparedStatement stmt;
	int branchingFactor;
	int height;
	String prefix="L";
	String separator="-";
	BufferedWriter bw=null;
	String path="D:\\\\workspaces\\\\AdvProject\\\\MBTDB\\\\mbtreeInternalLevel";
	String fileExtension=".txt";
	boolean autoCommit=true;
	public MBTreeManager(DBManager dbManager) {
		this.dbManager=dbManager;
		
	}

	public MBTreeManager(DBManager dbManager, int branchingFactor,int height) {
		this.dbManager=dbManager;
		this.branchingFactor=branchingFactor;
		this.height=height;
	}
	
	public void emptyMBTreeTable(){
		try {
			PreparedStatement delStmt = dbManager.getConnection().prepareStatement("delete from mbtree");
			delStmt.execute();
			delStmt.close();
		} 
		catch (SQLException e2) {
			
			e2.printStackTrace();
		}
		
	}

	public void populateMBTreeLeaves(int height){
		
		File file = new File("mbtreeInternalLevel"+height+".txt");
		try {
			if (!file.exists())
				file.createNewFile();
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			bw = new BufferedWriter(fw);
		}catch (IOException e) {
			e.printStackTrace();
		} 
		
		int numLeaves=(int)Math.pow(MBTCreator.branchingFactor,MBTCreator.height);
		try {
			PreparedStatement selStmt=dbManager.getConnection().prepareStatement("select value1 from btree where level_id=? and leaf_id=?");
			selStmt.setInt(1, height);
			for(int i=0;i<numLeaves;i++){
				selStmt.setInt(2,i);
				System.out.println("Executing Query on leaf "+i);
				ResultSet rs=selStmt.executeQuery();
				System.out.println("Executed query and got results");
				String sha1Hash="";
				while(rs.next()){
					String val=rs.getString(1);
					sha1Hash=sha1Hash+sha1(val);
				}
				insertMBTreeTableDetails(height, i, sha1Hash);
				
			} 
			selStmt.close();
			try {
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			loadDataIntoTable(height);
		}catch (SQLException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	public void populateMBTreeInternalNodes(int height,int branchingFactor){

		for(int i=height-1;i>=0;i--){
			System.out.println("Processing level "+i);
			File file = new File("mbtreeInternalLevel"+i+".txt");
			try {
				if (!file.exists())
					file.createNewFile();
				FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
				bw = new BufferedWriter(fw);
			}catch (IOException e) {
				e.printStackTrace();
			} 
			populateMBTreeLevel(i);
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			loadDataIntoTable(i);
		}
		
	}
	
	
	public void loadDataIntoTable(int i) {
		System.out.println("Loading level "+i);
		System.out.println(path+i+fileExtension);
		
		String tempQuery=" LOAD DATA LOCAL INFILE " + "\'"+path+i+fileExtension+"\'"+
                " INTO TABLE mbtreetemp " +
                " FIELDS TERMINATED BY \',\' " +
                " LINES TERMINATED BY \'\\n\'";
		String query="INSERT INTO mbtree SELECT * FROM mbtreetemp";
		String emptyTableTemp="delete from mbtreetemp";
		PreparedStatement stmt;
		try {
			stmt = dbManager.getConnection().prepareStatement(emptyTableTemp);
			stmt.executeUpdate();
			if(autoCommit==false)
				dbManager.getConnection().commit();
			stmt = dbManager.getConnection().prepareStatement(tempQuery);
			stmt.executeUpdate();
			if(autoCommit==false)
				dbManager.getConnection().commit();
			stmt=dbManager.getConnection().prepareStatement(query);
			stmt.executeUpdate();
			if(autoCommit==false)
				dbManager.getConnection().commit();
			
			System.out.println("Loading complete");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// TODO Auto-generated method stub
		
	}

	public void populateMBTreeLevel(int level){
		try {
			System.out.println("level is"+level);
			TreeMap<Integer,String> leafKeyMap=new TreeMap<Integer,String>();
			int curLevel=level;
			int nextLevel=level+1;
			
			PreparedStatement selStmt=dbManager.getConnection().prepareStatement("select leaf_id,hash_val from mbtree where level_id=? order by leaf_id");
			selStmt.setInt(1, nextLevel);
			ResultSet rs=selStmt.executeQuery();
			
			while(rs.next()){
				int leafId=rs.getInt(1);
				String hashVal=rs.getString(2);
				leafKeyMap.put(leafId, hashVal);	
			}
			
			Iterator<Integer> iter=leafKeyMap.keySet().iterator();
			int count=0;
			int curLeafId=0;
			String sha1Hash="";
			while(iter.hasNext()){
				if(count==branchingFactor){
					//if(level==3 && curLeafId==0 ){
					//	System.out.println("Level 3 culprit");
					//	System.out.println(sha1Hash);
						
					//}
					insertMBTreeTableDetails(curLevel, curLeafId, sha1Hash);
					//System.out.println(sha1Hash);
					//System.exit(0);
					sha1Hash="";
					count=0;
					curLeafId++;
					
				}
				int key=iter.next();
				String value=leafKeyMap.get(key);
				//if(level==3 && curLeafId==0 ){
					
					//System.out.println(key+" "+value);
					
				//}
				sha1Hash=sha1Hash+value;
				
				count++;
			}
			insertMBTreeTableDetails(curLevel, curLeafId, sha1Hash);
			System.out.println(sha1Hash);
			
			selStmt.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			
			e.printStackTrace();
		}
		
	}

	public void insertMBTreeTableDetails(int level_id,int leaf_id, String value) throws NoSuchAlgorithmException {	
		String sha1Hash=sha1(value);
		//insertData(level_id,leaf_id,sha1Hash);
		insertIntoFile(level_id,leaf_id,sha1Hash);
		System.out.println(level_id+" "+leaf_id+"  "+sha1Hash);
		if(level_id==0){
			MBTCreator.rootHash=sha1Hash;
			System.out.println(MBTCreator.rootHash);
		}
		//System.out.println(sha1Hash);
	}

	private void insertData(int level_id,int leaf_id, String hash) {
		try 
		{
			if(stmt==null){
			stmt = dbManager.getConnection().prepareStatement("insert into mbtree values(?,?,?)");
			}
			stmt.setInt(1, level_id);
			stmt.setInt(2, leaf_id);
			stmt.setString(3, hash);
			
			stmt.execute();
			
		} 
		catch (SQLException e2) {
			e2.printStackTrace();
		}
		
	}
	
	public String sha1(String input) {
        MessageDigest mDigest;
        StringBuffer sb = new StringBuffer();
		try {
			mDigest = MessageDigest.getInstance("SHA1");
			byte[] result = mDigest.digest(input.getBytes());
	        for (int i = 0; i < result.length; i++) {
	            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
	        }
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
         
        return sb.toString();
    }
	
	private void insertIntoFile(int levelId, int leafId, String value) {
		String content=levelId+","+leafId+","+value+"\n";
		// if file doesnt exists, then create it
		try {
			
			bw.write(content);
			
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		

		
		// TODO Auto-generated method stub
		
	}

}
