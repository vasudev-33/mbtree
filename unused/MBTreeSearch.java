package edu.sunysb.dbManager;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

public class MBTreeSearch {
	private static final int METADATA =1;
	//public static int LEFTKEY=1000002;
	//public static int RIGHTKEY=3234567;
	int resultCount=-1;
	int boundaryKeyCount=-1;
	int boundaryHashCount=-1;
	int boundaryKeyLeftLeafId=-1;
	int boundaryKeyRightLeafId=-1;
	HashMap<Integer,ArrayList<Node>> levelWiseNodeList=new HashMap<Integer,ArrayList<Node>>();
	int branchingFactor;
	int height;
	public static String rootHash;
	
	MBTreeSearch(int branchingFactor, int height){
		this.branchingFactor=branchingFactor;
		this.height=height;
	}
	
	public static void main(String args[]){
		String searchQuery="select ";
		DBManager dbManager =new DBManager();
		Connection connection=dbManager.openConnection();
		//Connection connection=dbManager.getConnection();
		//MBTreeSearch mbTreeSearch=new MBTreeSearch(branchingFactor, height);
		//mbTreeSearch.search(connection, searchQuery);
		dbManager.closeConnection();
	}
	
	
	public void search(Connection connection, CallableStatement callableStatement, int leftKey, int rightKey) throws IOException{
		
		try {
			String searchString="call search("+leftKey+","+rightKey+","+branchingFactor+","+height+")";
			//long startTime=System.currentTimeMillis();
			ResultSet rs=callableStatement.executeQuery(searchString);
			//long endTime=System.currentTimeMillis();
			//long diff=endTime-startTime;//TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
			//System.out.println("Time for Search= "+diff+" milliseconds");
			//MBTCreator.bw.write("Time for Search= "+diff+" milliseconds\n");
			ArrayList<Node> lastLevelNodeList=populateLastLevelNodeList(rs);
			if(lastLevelNodeList==null){
				return;
			}
			levelWiseNodeList.put(height,lastLevelNodeList);
			
			//send all the intermediate level hashes obtained from search to the respective list in the hashmap
			int count=0;
			while(count<boundaryHashCount){
				rs.next();
				rs.getString(1);
				String parts[]=rs.getString(1).split(",");
				int levelId=Integer.parseInt(parts[0]);
				int leafId=Integer.parseInt(parts[1]);
				String hashVal=parts[2];
				Node node=new Node(leafId,hashVal);
				if(levelWiseNodeList.containsKey(levelId)){
					ArrayList<Node> nodeList=levelWiseNodeList.get(levelId);
					nodeList.add(node);
					levelWiseNodeList.put(levelId, nodeList);
					
				}else{
					ArrayList<Node> nodeList=new ArrayList<Node>();
					nodeList.add(node);
					levelWiseNodeList.put(levelId, nodeList);
				}
				count++;
			}
			//sort all the lists in the hashmap
			Iterator<Integer> iter=levelWiseNodeList.keySet().iterator();
			while(iter.hasNext()){
				int levelId=iter.next();
				ArrayList<Node> nodeList=levelWiseNodeList.get(levelId);
				Collections.sort(nodeList);
				levelWiseNodeList.put(levelId, nodeList);
				
			}
			processLevelWiseNodeList();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	private void processLevelWiseNodeList() {
		for(int i=height;i>0;i--){
			
			Collections.sort(levelWiseNodeList.get(i));
			//printList(levelWiseNodeList.get(i));
			processLevelNodes(i,levelWiseNodeList.get(i));
		}
		rootHash=levelWiseNodeList.get(0).get(0).getHashVal();
		
		
	}

	private void processLevelNodes(int levelId, ArrayList<Node> nodeList) {
		//System.out.println("Processing level "+levelId);
		//System.out.println(levelId+" "+nodeList.size());
		//System.out.println("My parents");
		if(nodeList.size()!=0){
			//int startLeafId=nodeList.get(0).getLeafId();
			//System.out.println(nodeList.size());
			/*
			 * work around for off by 1 to left example search(6120001,6161691,25,4)
			 */
			int startId=0;
			//if(nodeList.get(0).leafId % MBTCreator.branchingFactor != 0)
			//	startId=1;
			//work around ends
			for(int i=startId;i<nodeList.size();i=i+branchingFactor){
				
				String hashVal="";
				int parentLeafId=nodeList.get(i).getLeafId()/branchingFactor;
				//System.out.println("i "+i);
				/*
				 * work around for off by 1 to right example search(6120002,6135000,25,4)
				 */
				//if(i+branchingFactor>nodeList.size())
					//continue;
				//workaround ends
				for(int j=i;j<i+branchingFactor;j++){
					
					//System.out.println("j "+j+" "+nodeList.get(j).leafId+" "+nodeList.get(j).hashVal);
					Node node=nodeList.get(j);
					hashVal=hashVal+node.getHashVal();
				}
				String unhashedVal=hashVal;
				hashVal=sha1(hashVal);
				Node node=new Node(parentLeafId,hashVal);
				//System.out.println(parentLeafId+" "+unhashedVal);
				//System.out.println(parentLeafId+" "+hashVal);
				if(levelWiseNodeList.containsKey(levelId-1)){
					ArrayList<Node> parentNodeList=levelWiseNodeList.get(levelId-1);
					int hasNodeWithLeaf=hasNodeWithLeafId(parentNodeList,parentLeafId);
					if(hasNodeWithLeaf!=-1){
						parentNodeList.remove(hasNodeWithLeaf);
					}
					parentNodeList.add(node);
					levelWiseNodeList.put(levelId-1, parentNodeList);
					
				}else{
					ArrayList<Node> parentNodeList=new ArrayList<Node>();
					parentNodeList.add(node);
					levelWiseNodeList.put(levelId-1, parentNodeList);
				}
				
			}
			
		}
		
	}

	private void printList(ArrayList<Node> nodeList){
		for(int i=0;i<nodeList.size();i++){
			System.out.println(nodeList.get(i).getLeafId()+" "+nodeList.get(i).getHashVal());
		}
	}
	
	private int hasNodeWithLeafId(ArrayList<Node> nodeList, int leafId){
		for(int i=0;i<nodeList.size();i++){
			if(nodeList.get(i).getLeafId()==leafId){
				return i;
			}
		}
		return -1;
	}
	
	private ArrayList<Node> populateLastLevelNodeList(ResultSet rs) throws IOException {
		// TODO Auto-generated method stub
		//System.out.println("Printing hashes for level 4");
		int rowCount;
		TreeMap<Integer,String> keyTreeMap = null;
		try {
			rowCount = getRowCount(rs);
			rowCount=rowCount-METADATA;
			
			if(rs.next()){
				String firstMetaRow=rs.getString(1);
				String parts[]=firstMetaRow.split(",");
				boundaryKeyCount=Integer.parseInt(parts[0]);
				boundaryHashCount=Integer.parseInt(parts[1]);
				boundaryKeyLeftLeafId=Integer.parseInt(parts[2]);
				boundaryKeyRightLeafId=Integer.parseInt(parts[3]);
				//System.out.println("boundaryKeyLeftLeafId="+boundaryKeyLeftLeafId);
				//System.out.println("boundaryKeyRightLeafId="+boundaryKeyRightLeafId);
				
			}
			
			if(boundaryKeyCount>=0 && boundaryHashCount>=0){
				resultCount=rowCount-(boundaryKeyCount+boundaryHashCount);
			}else{
				System.out.println("negative values in boundary key and boundary hash");
				return null;
			}
			//System.out.println("boundaryKeyCOunt="+boundaryKeyCount);
			//System.out.println("boundaryHashCOunt="+boundaryHashCount);
			//System.out.println("resultCount="+resultCount);
			//MBTCreator.bw.write("resultCount="+resultCount+"\n");
			keyTreeMap= new TreeMap<Integer,String>();
			//populating the leaf ids for results and boundary
			//rs.next();
			
			if(resultCount<=2){
				System.out.println("Invalid Query");
				
				return null;
			}
			if(resultCount>0){
				int count=0;
				while(count<resultCount&&rs.next()){
					//System.out.println(rs.getString(1));
					String parts[]=rs.getString(1).split(",");
					keyTreeMap.put(Integer.parseInt(parts[0]), sha1(parts[1]));
					count++;
				}
				count=0;
				while(count<boundaryKeyCount){
					if(rs.next()){
						//System.out.println(rs.getString(1));
						String parts[]=rs.getString(1).split(",");
						keyTreeMap.put(Integer.parseInt(parts[0]), parts[1]);
						count++;
					}
				}
				Iterator<Integer> iter=keyTreeMap.keySet().iterator();
				int boundaryLeafLeft=boundaryKeyLeftLeafId;
				ArrayList<Node> nodeList=new ArrayList<Node>();
				//System.out.println("\nLast level Leaf Hashes");
				while(iter.hasNext()){
					
					String hashVal="";
					for(int i=1;i<branchingFactor;i++){
						hashVal=hashVal+keyTreeMap.get(iter.next());
					}
					//System.out.println(hashVal);
					hashVal=sha1(hashVal);
					Node node=new Node(boundaryLeafLeft,hashVal);
					//System.out.println(boundaryLeafLeft+" "+hashVal);
					
					//System.out.println(boundaryLeafLeft+" "+hashVal);
					nodeList.add(node);
					boundaryLeafLeft++;
					
				}
				return nodeList;
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}

	public static int getRowCount(ResultSet set) throws SQLException {  
	   int rowCount;  
	   int currentRow = set.getRow();            // Get current row  
	   rowCount = set.last() ? set.getRow() : 0; // Determine number of rows  
	   if (currentRow == 0)                      // If there was no current row  
	      set.beforeFirst();                     // We want next() to go to first row  
	   else                                      // If there WAS a current row  
	      set.absolute(currentRow);              // Restore it  
	   return rowCount;  
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

class NodeComparator implements Comparator<Node> {
    @Override
	public int compare(Node n1, Node n2) {
        return n1.compareTo(n2);
    }
}
