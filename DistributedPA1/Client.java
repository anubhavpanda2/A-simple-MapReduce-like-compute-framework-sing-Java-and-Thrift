import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.util.*;
import java.io.*;
public class Client {
    public static void main(String [] args) {
        //Create client connect.
        try {
            TTransport  transport = new TSocket("localhost", 9095);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            SentimentAnalysisServer.Client client = new SentimentAnalysisServer.Client(protocol);

            //Try to connect
            transport.open();

            //What you need to do.
			client.ping();
			//System.out.println("Anubhavfirst");
			List<String> fileNames = new ArrayList<String>();
			File[] files = new File("./data/input_dir").listFiles();

			for (File file : files) {
				if (file.isFile()) {
					fileNames.add(file.getName());
			//		System.out.println(file.getName());
				}
			}
			//System.out.println("Anubhav");
			client.process(fileNames);
			
        } catch(TException e) {
			//System.out.println(e.getMessage());
        }

    }
}
