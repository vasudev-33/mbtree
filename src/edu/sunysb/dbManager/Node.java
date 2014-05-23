package edu.sunysb.dbManager;

public class Node implements Comparable {
		int leafId;
		String hashVal;
		public Node(int leafId,String hashVal){
			this.leafId=leafId;
			this.hashVal=hashVal;
		}
		public int getLeafId() {
			return leafId;
		}
		public void setLeafId(int leafId) {
			this.leafId = leafId;
		}
		public String getHashVal() {
			return hashVal;
		}
		public void setHashVal(String hashVal) {
			this.hashVal = hashVal;
		}
		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			Node n=(Node) o;
			if(this.getLeafId()<n.getLeafId())
				return -1;
			else if(this.getLeafId()==n.getLeafId())
				return 0;
			else 
				return 1;
		}
		
	}




