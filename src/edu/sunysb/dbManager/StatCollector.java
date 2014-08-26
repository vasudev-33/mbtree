package edu.sunysb.dbManager;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

public class StatCollector implements Runnable
{
	private boolean running;
	private final static int STAT_TIME = 5;
	private String label;
	private DecimalFormat df;
	
	public StatCollector(String label)
	{
		this.label = label;
		df = new DecimalFormat("#.000"); 
	}
	
	@Override
	public void run()
	{
		running = true;
		double data = 0;
		while(running)
		{
			//System.out.println("loop");
			try
			{
				Process p = Runtime.getRuntime().exec("vnstat -tr " + STAT_TIME);
				
	            BufferedReader in = new BufferedReader(
	                                new InputStreamReader(p.getInputStream()));  
	            String line = null;  
	            while ((line = in.readLine()) != null) 
	            {  
	                //System.out.println(line);
	                StringTokenizer tk = new StringTokenizer(line, " \t");
					if(tk.countTokens() > 1)
					{
						String firstToken = tk.nextToken();
						if(firstToken.equals("rx") || firstToken.equals("tx"))
						{
							String strData = tk.nextToken();
							String unit = tk.nextToken();
							if(unit.equals("kbit/s"))
								data += ((Double.parseDouble(strData) * STAT_TIME)/ 8);
							else
								data += ((Double.parseDouble(strData) * STAT_TIME) / 8);
						}
					}
	            }	            
	        }
			catch (IOException e)
	        {  
	            e.printStackTrace();  
	        }  
		}//end while
		
		System.out.println(label + "," + df.format(data));
	}
	
	public void stop()
	{
		running = false;
	}
}
