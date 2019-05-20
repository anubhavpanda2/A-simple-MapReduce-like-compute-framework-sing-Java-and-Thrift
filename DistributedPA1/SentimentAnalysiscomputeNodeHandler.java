import org.apache.thrift.TException;
import java.util.*;
import java.io.*;
import java.util.concurrent.TimeUnit;
import java.net.InetAddress;
import java.net.UnknownHostException;
public class SentimentAnalysiscomputeNodeHandler implements SentimentAnalysiscomputeNode.Iface
{

        @Override
        public boolean ping() throws TException {
			System.out.println("I got ping()");
			return true;
		}

        @Override
        public boolean ComputeMap(String fileName,double probability) throws TException {
			
			 // handler = new MapHandler();
           // processor = new CustomMap.Processor(handler);

		try{
			//Read Config File for testcase
			FileReader config = new FileReader("./data/config");
			Properties configProperties = new Properties();
			configProperties.load(config);
			String testcase = configProperties.getProperty("testcase");
			int rangeMin=0,rangeMax=1;
			Random r = new Random();
			
			//Get Host Name
			InetAddress ip;
        		String hostname="";
        		try {
            			ip = InetAddress.getLocalHost();
            			hostname = ip.getHostName();
        		} catch (UnknownHostException e) {
 
           			System.out.println("Hostname not found");
        		}


			//Read testcases
			FileReader reader = new FileReader("./data/testcases/"+testcase);
			Properties properties = new Properties();
			properties.load(reader);
			double nodeLoad = Double.parseDouble(properties.getProperty(hostname));

			//Rejecting Tasks
			if(probability!=0){
				double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
				if(randomValue<=nodeLoad){
		//			System.out.println("---------------------------Rejected File "+fileName+"------------------------------------");
					return false;
				}
			}
			//Load Injecting
			double randLoad = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
			if(randLoad<=nodeLoad){
				try{
			        TimeUnit.SECONDS.sleep(3);
				}catch(InterruptedException ex){
				}
				//System.out.println("Sleep");
			}
			
		}catch(Exception Ex){
			System.out.println("Error Reading Config File");
		}
		String positiveFileName = "./data/positive.txt";
		String negativeFileName = "./data/negative.txt";
		String computeFileName = "./data/input_dir/"+fileName;

		HashSet<String> positiveSet =  new HashSet<>();
		HashSet<String> negativeSet = new HashSet<>();

		File positiveFile = new File(positiveFileName);
		File negativeFile = new File(negativeFileName);
		File computeFile = new File(computeFileName);

		BufferedReader br;
		
		String line;
		try{
		br = new BufferedReader(new InputStreamReader(new FileInputStream(positiveFile)));
		line = null;

		while((line = br.readLine())!=null){
			String[] words = line.split("\\s+");
			for(String word : words)
				positiveSet.add(word);
		}
		}catch(Exception ex){
			System.out.println("Positive File Not Found");
		}

		System.out.println("Positive Set Size: "+positiveSet.size());

		try{
                br = new BufferedReader(new InputStreamReader(new FileInputStream(negativeFile)));
		line = null;

		while((line = br.readLine())!=null){
			String[] words = line.split("\\s+");
			for(String word : words)
				negativeSet.add(word);
		}
		}catch(Exception ex){
			System.out.println("Negative File Not Found");
		}

		int positiveCnt = 0, negativeCnt = 0, neutralCnt = 0;
		long startTime = System.currentTimeMillis();
		try{
		br = new BufferedReader(new InputStreamReader(new FileInputStream(computeFile)));
		//br = new BufferedReader(new InputStreamReader( new FileInputStream("./data/example/"+fileName)));
		line = null;
		while((line = br.readLine())!=null){
			line = line.replaceAll("--", " ");
			line = line.replaceAll("'"," ");
			line = line.replaceAll("[^a-zA-Z0-9\\- ]","");
			String[] words = line.split("\\s+");
			for(String word : words){
				//if(word.contains("'")){
				 // word = word.substring(0,word.indexOf("'"));
				//}
				word = word.toLowerCase();
				if(positiveSet.contains(word))
					positiveCnt++;
				if(negativeSet.contains(word))
					negativeCnt++;
			}
		}
		}catch(Exception ex){
			System.out.println("Compute File :"+fileName+" Not Found");
		}

		double sentimentScore = ((positiveCnt-negativeCnt)*1.0)/(positiveCnt+negativeCnt);
		long endTime = System.currentTimeMillis();
		//System.out.println("No of neutral words = "+neutralCnt);
		System.out.println(fileName);
		System.out.println("Positive Word: "+positiveCnt);
		System.out.println("Negative Word: "+negativeCnt);
		//System.out.println("Neutral Word: "+neutralCnt);
		System.out.println("Sentiment Score = "+sentimentScore);
		System.out.println("Time taken : "+(endTime-startTime)+"ms");
		System.out.println();
		System.out.println();


		try{
		String intermediatePath = "./data/intermediateFiles/";
		File intermediateFile = new File(intermediatePath+fileName);//".txt");

		FileWriter writer = new FileWriter(intermediateFile);
		writer.write(fileName+"\t"+sentimentScore);
		writer.close();
		}catch(Exception ex){
		  System.out.println("Exception in writing intermediate file");
		}
		//System.out.println("computeNode"+fileName);
		return true;
        }
		@Override
		public boolean Computesort() throws TException{

                        System.out.println("Sort task started...");
			long startTime = System.currentTimeMillis();
			String filePath = "./data/intermediateFiles/";
			TreeMap<Double,ArrayList<String>> map = new TreeMap<>();
			File[] files = new File(filePath).listFiles();
			//System.out.println(files.length);
			BufferedReader br=null;
			for(File file : files){
				//System.out.println(file.getName());
				try{	
				br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
				String line = null;
				while((line=br.readLine())!=null){
					String[] words = line.split("\\s+");
					if(!map.containsKey(Double.parseDouble(words[1]))){
						map.put(Double.parseDouble(words[1]),new ArrayList<String>());
						map.get(Double.parseDouble(words[1])).add(words[0]);
					}else{
						map.get(Double.parseDouble(words[1])).add(words[0]);
					}
				}
				}catch(Exception ex){
					System.out.println("IntermediateFile: "+file.getName()+" not found exception");
				}
			}
			String outputPath = "./data/output_dir/";
			try{
			File outputFile = new File(outputPath+"output.txt");
			FileWriter writer = new FileWriter(outputFile);
			for(Map.Entry<Double,ArrayList<String>> entry : map.entrySet()){
				for(String value : entry.getValue()){
					writer.write(value+"\t\t"+entry.getKey()+"\n");
					//writer.newLine();
					//System.out.println(value+"\t"+entry.getKey());
				}
			}
			long endTime = System.currentTimeMillis();
			System.out.println("Time taken to sort : "+(endTime-startTime));
			writer.close();
			}catch(Exception ex){
				System.out.println("Output file not found");
			}
			
			return true;
		}
        
        
}
