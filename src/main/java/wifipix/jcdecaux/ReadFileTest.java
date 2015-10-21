package wifipix.jcdecaux;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by aurora on 15/10/20.
 */
public class ReadFileTest {


    public static void main(String[] args) throws Exception {
        String readFileName = "/Users/aurora/Desktop/format918.log";
        File file = new File(readFileName);
        BufferedReader reader = null;
        String tmpline = null;
        Map<String, Integer> allMac = new HashMap<String, Integer>();
        long sum = 0;

        long allnum = 0;
        String theSpecialAP = "e4:95:6e:4f:54:95";
        String timeStart800 = "2015-09-18 08:00:00";
        String timeend935 = "2015-09-18 09:35:00";

        try {
            reader = new BufferedReader(new FileReader(file));
            while ((tmpline = reader.readLine()) != null) {
                String[] field = tmpline.split(" ");

                if (iswantTime(timeStart800, timeend935, field[0])) {
                    if (field[1].equals(theSpecialAP)) {
//                        System.out.println(tmpline);
                        allMac.put(field[2], 1);
                    }
                }

            }

            Iterator iterator = allMac.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();
                 sum += 1;
            }
            System.out.println("all the num is " + sum);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static boolean iswantTime(String star,String end, String datatime){
        SimpleDateFormat localTime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try{
            Date sdate = localTime.parse(star);
            Date edate=localTime.parse(end);
            Date date = localTime.parse(toLocalTime(datatime));
//            System.out.println(sdate.getTime()+"##"+date.getTime()+"##"+edate.getTime());
            if (date.after(sdate) && date.before(edate)) {
//                    System.out.println("true");
                return true;
            }
        }catch(Exception e){}

        return false;
    }
    //转换为本地时间
    public static String toLocalTime(String unix) {
        Long timestamp = Long.parseLong(unix) * 1000;
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(timestamp));
        return date;
    }


    public static String toUnixTime(String local){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String unix = "";
        try {
            unix = df.parse(local).getTime() + "";
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return unix;
    }

}
