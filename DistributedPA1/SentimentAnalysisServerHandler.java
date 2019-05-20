import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.TException;
import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.util.concurrent.TimeUnit;
public class SentimentAnalysisServerHandler implements SentimentAnalysisServer.Iface
{

        @Override
        public boolean ping() throws TException {
			System.out.println("I got ping()");
			return true;
		}

        @Override
        public boolean process(List<String>fileNames) throws TException {

			//Read Config File
			Properties properties = null;
			try{
				FileReader reader = new FileReader("./data/config");
				properties = new Properties();
				properties.load(reader);
			}catch(Exception fr){
				System.out.println("Exception in reading config file");
			}
			final double mode = Double.parseDouble(properties.getProperty("mode"));
			ArrayList<String> Address=new ArrayList<String>();
			Address.add(properties.getProperty("node1"));
			Address.add(properties.getProperty("node2"));
			Address.add(properties.getProperty("node3"));
			Address.add(properties.getProperty("node4"));
			//Address.add("csel-kh4250-03.cselabs.umn.edu");
			//Address.add("csel-kh4250-06.cselabs.umn.edu");
			//Address.add("csel-kh4250-10.cselabs.umn.edu");
			Queue<String> q = new LinkedList<String>();
			System.out.println("Job Submitted. Total tasks = "+fileNames.size());
			long mapStartTime = System.currentTimeMillis();
			for(String file:fileNames)
				q.add(file); 
			boolean flag=false;
		Thread t = null;	
			while(!q.isEmpty())	
			{
				//System.out.println("new"+q.size());
				String file=q.remove();
				
					try{
						Runnable simple = new Runnable() {
							public void run() {
								String f=file;
									//System.out.println(file);
									Random rand = new Random();
									int n = rand.nextInt(4);
									//int n=1;
									//n+=1;
									TTransport  transport = new TSocket(Address.get(n), 9099);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            SentimentAnalysiscomputeNode.Client client = new SentimentAnalysiscomputeNode.Client(protocol);
			try
			{	
            transport.open();
			boolean response =true;
			double d=mode; 
			
			response=client.ComputeMap(f,d);
			if(response==false) 
			{
				//Need to add this inside wait for 1 sec to handle response of true
			System.out.println("Retry "+f);
				q.add(f);
			}
				
			}
			
			catch(Exception e)
			{
				q.add(file);
				e.printStackTrace();
				System.out.println("innr Exception"+e.getMessage()+" "+Address.get(n));
			}
			
			
						}
            };
	t = new Thread(simple);
	t.start();

					}
			catch(Exception e){
				System.out.println("Exception");
			}

	 while(q.isEmpty())
	{
					File[] files = new File("./data/intermediateFiles").listFiles();
	if(files.length==fileNames.size())
	{
		break;
	}
	}
	}//End of While loop
			try
			{
				t.join();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
	File[] files = new File("./data/intermediateFiles").listFiles();
	if(files.length!=fileNames.size())
	{
	    System.out.println("Tasks completed = "+files.length+"\nTasks Remaining = "+(fileNames.size()-files.length));
	}	    
	    TTransport  transport = new TSocket("csel-kh4250-03.cselabs.umn.edu", 9099);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            SentimentAnalysiscomputeNode.Client client = new SentimentAnalysiscomputeNode.Client(protocol);
	    long mapEndTime = System.currentTimeMillis();
	    //System.out.println("Map tasks Completed...\nTime taken for Map task = "+(mapEndTime-mapStartTime)+"\nStarting Sort task...");

	    long sortStartTime = System.currentTimeMillis();
            //Try to connect
            transport.open();
	    client.Computesort();
	    long sortEndTime = System.currentTimeMillis();
	    System.out.println("Time taken for Map task = "+(mapEndTime-mapStartTime)+"ms");
	    System.out.println("Time taken for Sort  task ="+(sortEndTime-sortStartTime)+"ms");
	    System.out.println("Job completed...");
	    System.out.println("Total Time Taken for the job to complete = "+(sortEndTime-mapStartTime)+"ms");	    

	    return true;
        }
        
        
}
