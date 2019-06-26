package data;

import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.model.AdsEventGenerator;
import data.source.model.EventGenerator;
import data.source.socket.BufferReaderThread;
import data.source.socket.DataGeneratorThread;

import java.io.FileReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Main {
    public static EventGenerator parseEventGenerator(String[] args) throws Exception {
        EventGenerator selected;
        double partition = new Double(args[2]);

        switch (args[1]) {
            case "ads":
                selected = new AdsEventGenerator(partition);
                break;
            default:
                throw new Exception("No model selected!");
        }

        return selected;
    }

    public static void main(String[] args) throws Exception {
        String confFilePath = args[0];
        EventGenerator eventGenerator = parseEventGenerator(args);
        YamlReader reader = new YamlReader(new FileReader(confFilePath));
        Object object = reader.read();
        HashMap conf = (HashMap) object;

        Integer port = new Integer(conf.get("datasourcesocket.port").toString());
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(900000);
        System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
        Socket server = serverSocket.accept();
        System.out.println("Just connected to " + server.getRemoteSocketAddress());
        PrintWriter out = new PrintWriter(server.getOutputStream(), true);
        int bufferSize = new Integer(conf.get("benchmarking.count").toString());
        BlockingQueue<String> buffer = new ArrayBlockingQueue<String>(bufferSize);    // new LinkedBlockingQueue<>();
        try {
            Thread generator = new DataGeneratorThread(eventGenerator,conf, buffer);
            generator.start();
            Thread bufferReader = new BufferReaderThread(buffer, conf, out);
            bufferReader.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

