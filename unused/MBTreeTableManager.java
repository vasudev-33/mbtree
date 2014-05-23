package edu.sunysb.dbManager;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

public class MBTreeTableManager {
	DBManager dbManager;
	PreparedStatement stmt;
	int branchingFactor;
	int height;
	String prefix="L";
	String separator="-";
	
	public MBTreeTableManager(DBManager dbManager) {
		this.dbManager=dbManager;
		
	}

	public MBTreeTableManager(DBManager dbManager, int branchingFactor,int height) {
		this.dbManager=dbManager;
		this.branchingFactor=branchingFactor;
		this.height=height;
	}
	
	public void emptyMBTreeTable(){
		try {
			PreparedStatement delStmt = dbManager.getConnection().prepareStatement("delete from mbtree_table");
			delStmt.execute();
			delStmt.close();
		} 
		catch (SQLException e2) {
			
			e2.printStackTrace();
		}
		
	}

	public void populateMBTreeLeaves(){
		try {
			PreparedStatement selStmt=dbManager.getConnection().prepareStatement("select leaf_id from data");
			ResultSet rs=selStmt.executeQuery();
			ArrayList keyList=new ArrayList();
			while(rs.next()){
				int key=rs.getInt(1);
				keyList.add(key);
			}
			for(int i=0;i<keyList.size();i++){
				String key=prefix+height+separator+i;
				insertMBTreeTableDetails(key, ((Integer)i).toString());
			}
			selStmt.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			
			e.printStackTrace();
		}
		
	}
	
	public void populateMBTreeTableLevel(int level){
		try {
			int curLevel=level;
			int nextLevel=level+1;
			
			String nodeIdQuery=prefix+nextLevel+separator+'%';
			List keyList=new ArrayList();
			HashMap<Integer,String> resMap=new HashMap<Integer,String>();
			
			PreparedStatement selStmt=dbManager.getConnection().prepareStatement("select node_id,sha1_hash from mbtree_table where node_id like ?");
			selStmt.setString(1, nodeIdQuery);
			ResultSet rs=selStmt.executeQuery();

			while(rs.next()){
				String nodeId=rs.getString(1);
				String sha1Hash=rs.getString(2);
				String parts[]=nodeId.split(separator);
				resMap.put(Integer.parseInt(parts[1]), sha1Hash);
				keyList.add(Integer.parseInt(parts[1]));
			}
			
			int nodeNumber=0;
			for(int i=0;i<keyList.size();i=i+branchingFactor){
				int count=0;
				StringBuffer str=new StringBuffer();
				while(count<branchingFactor){
					str.append(resMap.get(i+count));
					count++;
				}
				String key=prefix+curLevel+separator+nodeNumber;
				System.out.println(key + " "+ str);
				insertMBTreeTableDetails(key, str.toString());
				nodeNumber++;
			}
			selStmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void insertMBTreeTableDetails(String key,String value) throws NoSuchAlgorithmException {	
		String sha1Hash=sha1(value);
		insertData(key,sha1Hash);
		System.out.println(key+"  "+sha1Hash);
		//System.out.println(sha1Hash);
	}

	private void insertData(String key, String hash) {
		try 
		{
			if(stmt==null){
			stmt = dbManager.getConnection().prepareStatement("insert into mbtree_table values(?,?)");
			}
			stmt.setString(1, key);
			stmt.setString(2, hash);
			
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

}
