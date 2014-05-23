package edu.sunysb.dbManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class FileReader1 {
	public static void main(String args[]){
		File file = new File("randomNumberTable\\sortedRandomNumberFile0.txt");
		try {
			if (!file.exists())
				file.createNewFile();
			FileReader fw = new FileReader(file);
			BufferedReader bw = new BufferedReader(fw);
			int count=0;
			String line;
			while((line=bw.readLine()) != null){
				
				//String line=bw.readLine();
				int i=Integer.parseInt(line);
				System.out.println(i);
				count++;
				//if(count==390360){
				//	System.out.println(line);
				//}
			}
			System.out.println(count);
		}catch (IOException e) {
			e.printStackTrace();
		} 
	}

}
