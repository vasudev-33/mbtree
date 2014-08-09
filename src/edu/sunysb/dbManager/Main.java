package edu.sunysb.dbManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Main {
	
	public final boolean UDBG=false;
	public void driver(int totalThreads, int range, int totalIterations){
		
		int totalKeys=70000000;
		//int totalThreads=numThreads;
		//int numThreads=0;
		//int totalIterations=1000;
		int numIters=totalIterations/totalThreads;
		int threadShare=totalKeys/totalThreads;
		int low=2;
		int high;
		//int range=100000;
		char charToFill='v';
		int newValLength=50;
		
		float avergaeUpdatesPerSecond=0;
		
		//System.out.println("Number of leaves="+numLeaves);
		//System.out.println("Number of keys="+mbtCreator.numKeys);
		try{
			if(UDBG){
				FileWriter fw = new FileWriter("summary-"+range+"-"+totalThreads+".csv");
				fw.write("Thread Id"+","+"Query Range"+","+"Number of Updates"+","+"Time(in ms)"+","+"Updates Per Second"+"\n");
				fw.close();
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		//Main main=new Main();
		String newValForUpdate=fill(newValLength, 'v');
		MBTCreator[] mbtThreads=new MBTCreator[totalThreads];
		//for(int i=100;i<=100000;i=i*10){
		for(int i=0;i<totalThreads;i++){
			MBTCreator mbtCreator= new MBTCreator();
			int numLeaves=(int) Math.pow(mbtCreator.branchingFactor,mbtCreator.height);
			mbtCreator.numKeys=numLeaves*(mbtCreator.branchingFactor-1);
			mbtCreator.threadId=i+1;
			mbtCreator.numRuns=numIters;
			mbtCreator.range=range;
			mbtCreator.newValueToUpdate=newValForUpdate;
			mbtCreator.threadLow=low;
			mbtCreator.totalThreads=totalThreads;
			high=low+threadShare-1;
			mbtCreator.threadHigh=high;
			low=high+1;
			
			mbtCreator.start();
			mbtThreads[i]=mbtCreator;
			
		}
		
		
		for(int i=0;i<mbtThreads.length;i++){
			try {
				mbtThreads[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		//System.out.println("Everyone is done");
		//System.out.println(MBTCreator.avgUpdatesPerSecond);
		
		try{
			if(UDBG){
			MBTCreator.avgUpdatesPerSecond=MBTCreator.avgUpdatesPerSecond/(float)totalThreads;
			FileWriter fw = new FileWriter("summary-"+range+".csv",true);
			BufferedWriter sbw=new BufferedWriter(fw);
			sbw.write(totalThreads+","+MBTCreator.avgUpdatesPerSecond+"\n");
			sbw.close();
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public String fill(int length, char c) {
	    StringBuilder sb = new StringBuilder(length);
	    while (sb.length() < length) {
	        sb.append(c);
	    }
	    return sb.toString();
	}
	
	public static void main(String args[]){
		Main main=new Main();
		int startThreads=1;
		int maxThreads=32;
		int totalIterations=32;
		int startRange=10;
		int endRange=10;
		
		for(int j=startRange;j<=endRange;j=j*10){
			for(int i=startThreads;i<=maxThreads;i=i*2){
				System.out.println("Performing a run with "+i+" threads, range: "+j+" iterations: "+totalIterations);
				long startTime=System.currentTimeMillis();
				main.driver(i,j,totalIterations);
				long endTime=System.currentTimeMillis();
				long diff = endTime-startTime;
				float numUpdatesPerSecond=((float)totalIterations/(float)diff)*1000;
				System.out.println("Completed a run with "+i+" threads. Time Taken= "+TimeUnit.MILLISECONDS.toSeconds(endTime-startTime)+" seconds. "+"Num updates= "+numUpdatesPerSecond+" per second.");
				System.out.println();
			}
		}
	}

}
