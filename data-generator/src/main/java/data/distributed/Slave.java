package data.distributed;

import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.model.AdsEventGenerator;
import data.source.model.EventGenerator;
import data.source.socket.BufferReaderThread;
import data.source.socket.DataGeneratorThread;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class Slave {


    public static void main(String[] args) throws Exception {
        String confFilePath = args[0];

        YamlReader reader = new YamlReader(new FileReader(confFilePath));
        Object object = reader.read();
        HashMap conf = (HashMap) object;

        String masterIp = String.valueOf(conf.get("mastersocket.ip"));
        Integer masterPort = new Integer(conf.get("mastersocket.port").toString());
        Socket masterConnection = new Socket(masterIp, masterPort);

        Integer port = new Integer(conf.get("datasourcesocket.port").toString());
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(900000);
        System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
        Socket server = serverSocket.accept();
        System.out.println("Just connected to " + server.getRemoteSocketAddress());

        PrintWriter out = new PrintWriter(server.getOutputStream(), true);

        while (true) {
            ObjectInputStream ois = new ObjectInputStream(masterConnection.getInputStream());
            TestRequest requestedTest = (TestRequest) ois.readObject();
            ois.close();
            TestResult result = executeRequestedTest(requestedTest, conf, out);
            ObjectOutputStream oos = new ObjectOutputStream(masterConnection.getOutputStream());
            oos.writeObject(result);

        }

    }

    private static TestResult executeRequestedTest(TestRequest requestedTest, HashMap conf, PrintWriter out) {
        int bufferSize = new Integer(conf.get("benchmarking.count").toString());

        ArrayBlockingQueue<String> buffer = new ArrayBlockingQueue<String>(bufferSize);
        try {
            DataGeneratorThread generator = new DataGeneratorThread(requestedTest.getGeneratorToUse(), conf, buffer);
            generator.start();
            BufferReaderThread bufferReader = new BufferReaderThread(buffer, conf, out, false);
            bufferReader.start();


        } catch (IOException e) {
            e.printStackTrace();
        }
        return new TestResult(true, null);
    }
}
