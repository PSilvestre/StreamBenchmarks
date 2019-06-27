package data.distributed;

import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.model.AdsEventGenerator;
import data.source.model.EventGenerator;

import java.io.FileReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Slave {

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

        String masterIp = String.valueOf(conf.get("mastersocket.ip"));
        Integer masterPort = new Integer(conf.get("mastersocket.port").toString());
        Socket masterConnection = new Socket(masterIp, masterPort);


    }

}
