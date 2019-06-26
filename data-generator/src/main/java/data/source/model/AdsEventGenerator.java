package data.source.model;

/**
 * Created by jeka01 on 02/09/16.
 */

import org.fluttercode.datafactory.impl.DataFactory;
import org.json.JSONObject;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by jeka01 on 31/08/16.
 */
public class AdsEventGenerator implements EventGenerator, Serializable {
    private int geoIndex = 0;
    private Random rand = new Random(93285L);

    private String[] geoListAll = {
            "AF", "AX", "AL", "DZ", "AS", "AD", "AO", "AI", "AQ", "AG", "AR", "AM", "AW", "AC", "AU", "AT", "AZ", "BS", "BH", "BB",
            "BD", "BY", "BE", "BZ", "BJ", "BM", "BT", "BW", "BO", "BA", "BV", "BR", "IO", "BN", "BG", "BF", "BI", "KH", "CM", "CA",
            "CV", "KY", "CF", "TD", "CL", "CN", "CX", "CC", "CO", "KM", "CG", "CD", "CK", "CR", "CI", "HR", "CU", "CY", "CZ", "CS",
            "DK", "DJ", "DM", "DO", "TP", "EC", "EG", "SV", "GQ", "ER", "EE", "ET", "EU", "FK", "FO", "FJ", "FI", "FR", "FX", "GF",
            "PF", "TF", "MK", "GA", "GM", "GE", "DE", "GH", "GI", "GB", "GR", "GL", "GD", "GP", "GU", "GT", "GG", "GN", "GW", "GY"};
    private String[] geoList = null;

    public AdsEventGenerator(Double partition) {
        if (partition > 0){
            geoList = Arrays.copyOfRange(geoListAll, 0, (int) (geoListAll.length * partition));
        } else{
            geoList = Arrays.copyOfRange(geoListAll, (int) (geoListAll.length * (1 + partition)), geoListAll.length);
        }

    }


    @Override
    public Event generateEvent() {
        String geo = null;
        geoIndex = geoIndex % geoList.length;
        geo = geoList[geoIndex];
        geoIndex++;

        //price
        float minX = 5.0f;
        float maxX = 100.0f;
        float finalX = rand.nextFloat() * (maxX - minX) + minX;
        String price = Float.toString(finalX);

        Event event = new Event(geo, price);
        return event;

    }
}

