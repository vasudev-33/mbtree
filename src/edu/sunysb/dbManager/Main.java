package edu.sunysb.dbManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Main {
	
	public final boolean UDBG=false;
	int leftElements[];
	int rightElements[];
	
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
	
	
	/* this functions returns the randomGenerationTime for run which is to be subtracted from total time */
	public long driver(int totalThreads, int range, int totalIterations, int runType){
		
		int totalKeys=72983762;
		
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
		
		int threadStartIndex=0;
		for(int i=0;i<totalThreads;i++){
			
			int[] clonedLeft=leftElements.clone();
			int[] clonedRight=rightElements.clone();
			
			MBTCreator mbtCreator= new MBTCreator();//clonedLeft,clonedRight);
			int numLeaves=(int) Math.pow(mbtCreator.branchingFactor,mbtCreator.height);
			mbtCreator.numKeys=numLeaves*(mbtCreator.branchingFactor-1);
			mbtCreator.threadId=i+1;
			mbtCreator.numRuns=numIters;
			mbtCreator.range=range;
			mbtCreator.newValueToUpdate=newValForUpdate;
			mbtCreator.threadLow=low;
			mbtCreator.totalThreads=totalThreads;
			mbtCreator.runType=runType;
			high=low+threadShare-1;
			mbtCreator.threadHigh=high;
			mbtCreator.threadStartIndex=threadStartIndex;
			low=high+1;
			
			mbtCreator.start();
			threadStartIndex+=numIters;
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
		
		System.out.println("Congestion Count="+MBTCreator.congestionCount);
		
		long maxRandomTimeForRun=0;
		for(int i=0;i<mbtThreads.length;i++){
			if(mbtThreads[i].randomGenerationTime>maxRandomTimeForRun)
				maxRandomTimeForRun=mbtThreads[i].randomGenerationTime;
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
		return maxRandomTimeForRun;
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
		int startThreads=Integer.parseInt(args[1]);
		int maxThreads=Integer.parseInt(args[2]);
		int totalIterations=Integer.parseInt(args[3]);
		int startRange=100;
		int endRange=100;
		int runType=Integer.parseInt(args[0]);
		
	
		for(int j=startRange;j<=endRange;j=j*10){
			
			/* generate all numbers for a range */
			main.leftElements = new int[totalIterations];
			main.rightElements = new int[totalIterations];
			int minKey=2;
			int maxKey=72980000;
			/*
			for(int i=0;i<totalIterations;i++){
				int leftKey;
				int rightKey;
				do{
					leftKey=main.randInt(minKey,maxKey-1);
					rightKey=main.randInt(leftKey+1,maxKey);
				}while((rightKey-leftKey)>j || leftKey==-1 || rightKey==-1);
				main.leftElements[i]=leftKey;
				main.rightElements[i]=rightKey;
			}
			// rand generation ends 
			for(int i=0;i<main.leftElements.length;i++){
				System.out.print(main.leftElements[i]+",");
			}
			System.out.println();
			for(int i=0;i<main.rightElements.length;i++){
				System.out.print(main.rightElements[i]+",");
			}
			System.exit(0);
			*/
			
			FileWriter fw;
			BufferedWriter bw=null;
			BufferedWriter bw1=null;
			try {
				fw = new FileWriter("type"+runType+"-range"+j+".csv",true);
				bw = new BufferedWriter(fw);
				bw1 = new BufferedWriter(new FileWriter("data-type"+runType+"-range"+j+".csv",true));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			for(int i=startThreads;i<=maxThreads;i=i*2){
				
				/*try {
					
					Runtime runtime=Runtime.getRuntime();
					runtime.exec("sudo service mysql stop");
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}*/
				
				//System.out.println("Performing a run with "+i+" threads, range: "+j+" iterations: "+totalIterations);
				
				StatCollector sc=new StatCollector(((Integer)i).toString());
				Thread statCollector=new Thread(sc);
				statCollector.start();
				long startTime=System.currentTimeMillis();
				long randomGenerationTimeForRun=main.driver(i,j,totalIterations,runType);
				long endTime=System.currentTimeMillis();
				sc.stop();
				try {
					statCollector.join();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				long diff = endTime-startTime;
				diff=diff-randomGenerationTimeForRun;
				float numUpdatesPerSecond=((float)totalIterations/(float)diff)*1000;
				//System.out.println("Completed a run with "+i+" threads. Time Taken= "+TimeUnit.MILLISECONDS.toMillis(diff)+" seconds. "+"Num updates= "+numUpdatesPerSecond+" per second.");
				System.out.println(i+","+numUpdatesPerSecond+" "+"seconds");
				//System.out.println("Rand Generation Time= "+TimeUnit.MILLISECONDS.toMillis(randomGenerationTimeForRun));
				//System.out.println("data transferred with " + i + " threads " + data+" KB");
				try {
					bw.write(i+","+numUpdatesPerSecond+"\n");
					//bw1.write(i+","+data+"\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				System.out.println();
			}
			
			try {
				bw.close();
				bw1.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
