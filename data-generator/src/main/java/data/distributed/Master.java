package data.distributed;

import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.model.AdsEventGenerator;
import data.source.model.EventGenerator;
import data.source.socket.BufferReaderThread;
import data.source.socket.DataGeneratorThread;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Master {
    private static final double START_THROUGHPUT = 5000;
    private static final double START_THROUGHPUT_DECREMENT = 512;
    private static final double STOP_THROUGHPUT_DECREMENT = 32;

    public static EventGenerator parseEventGenerator(String[] args) throws Exception {
        EventGenerator selected;
        double partition = new Double(args[2]);

        switch (args[1]) {
            case "ads":
                selected = new AdsEventGenerator(partition);
                break;
            default:
                throw new Exception("Invalid model selected!");
        }

        return selected;
    }


    public static void main(String[] args) throws Exception {
        String confFilePath = args[0];
        EventGenerator eventGenerator = parseEventGenerator(args);
        YamlReader reader = new YamlReader(new FileReader(confFilePath));
        Object object = reader.read();
        HashMap conf = (HashMap) object;

        Integer masterPort = new Integer(conf.get("mastersocket.port").toString());
        ServerSocket masterSocket = new ServerSocket(masterPort);
        masterSocket.setSoTimeout(1000);

        Set<Socket> slaves = new HashSet<>();

        long timeToWait = 10* 1000; //10 seconds

        long startTimeWaitForSlaves = System.currentTimeMillis();
        while(System.currentTimeMillis() < startTimeWaitForSlaves + timeToWait){
            try{
                slaves.add(masterSocket.accept());
            }catch (SocketTimeoutException e){

            }
        }
        System.out.println("Done waiting for slaves. " + slaves.size()  + " slaves connected.");

        System.out.println("Finding sustainable throughput...");
        double sustainableThroughput = findSustainableThroughput(slaves, eventGenerator);

        System.out.println("Sustainable throughput found: " + sustainableThroughput);
        //do actual experiment

        long testDuration = (Long) conf.get("testduration");

        TestRequest request = new TestRequest(sustainableThroughput, slaves.size(), testDuration, eventGenerator);
        for (Socket slaveConnection : slaves) {
            ObjectOutputStream oos = new ObjectOutputStream(slaveConnection.getOutputStream());
            oos.writeObject(request);
            oos.close();
        }
        Set<TestResult> results = new HashSet<>();
        for (Socket slaveConnection : slaves) {
            ObjectInputStream ois = new ObjectInputStream(slaveConnection.getInputStream());
            TestResult result = (TestResult) ois.readObject();
            ois.close();
            results.add(result);
        }
    }

    static boolean testThroughput(double throughput, Set<Socket> slaves, EventGenerator eventGenerator) throws IOException, ClassNotFoundException {
        TestRequest request = new TestRequest(throughput, slaves.size(), 20000,eventGenerator);
        for (Socket slaveConnection : slaves) {
            ObjectOutputStream oos = new ObjectOutputStream(slaveConnection.getOutputStream());
            oos.writeObject(request);
            oos.close();
        }
        Set<TestResult> results = new HashSet<>();
        for (Socket slaveConnection : slaves) {
            ObjectInputStream ois = new ObjectInputStream(slaveConnection.getInputStream());
            TestResult result = (TestResult) ois.readObject();
            ois.close();
            results.add(result);
        }

        if(results.stream().anyMatch(o -> !o.isSustainable()))
            return  false;
        return true;

    }

    private static double findSustainableThroughput(Set<Socket> slaves, EventGenerator eventGenerator) throws IOException, ClassNotFoundException {

        double throughput = START_THROUGHPUT;
        double throughputDecrement = START_THROUGHPUT_DECREMENT;


        while(true) {


            boolean sustainable = testThroughput(throughput, slaves, eventGenerator);

            if(sustainable && throughputDecrement <= STOP_THROUGHPUT_DECREMENT){
                break;
            }else if(sustainable) {
                throughput += throughputDecrement;
                throughputDecrement = throughputDecrement / 2;
            }

            throughput -= throughputDecrement;
        }
        return throughput;
    }

}
