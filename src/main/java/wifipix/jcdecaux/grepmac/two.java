package wifipix.jcdecaux.grepmac;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by aurora on 15/10/20.
 */
public class two {
    public static class getSimple2Mapper extends Mapper<Object, Text, Text, Text> {
        private Text getdata = new Text();
        private IntWritable one = new IntWritable(1);
        private Text rawData = new Text();
        private Text getTime = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] field = line.split(" ");

//            确认以下时间段，这2个MAC地址（ac:f7:f3:5a:db:30，D4:F4:6F:B8:68:B6）
//            是否有被尾号‘54：67’的探针所侦测到；
//            9:50-9:54
//            9:54-10:00

            String theSpecialAP = "e4:95:6e:4f:54:67";
            String theMi = "ac:f7:f3:5a:db:30";
            String theIphone = "d4:f4:6f:b8:68:b6";
            String timeStart = "2015-09-18 09:50:00";
            String timeend = "2015-09-18 09:54:00";

            String timeStart2 = "2015-09-18 09:54:00";
            String timeend2 = "2015-09-18 10:00:00";

            if (iswantTime(timeStart, timeend, field[0])) {
                if (field[1].equals(theSpecialAP)) {
                    if (field[2].equals(theIphone) || field[2].equals(theMi)) {
                        getTime.set("getTime1");
                        rawData.set(field[2]);
                        context.write(getTime,rawData);
                    }
                }
            }

            if (iswantTime(timeStart2, timeend2, field[0])) {
                if (field[1].equals(theSpecialAP)) {
                    if (field[2].equals(theIphone) || field[2].equals(theMi)) {
                        getTime.set("getTime2");
                        rawData.set(field[2]);
                        context.write(getTime,rawData);
                    }
                }
            }

        }

        public static boolean iswantTime(String star,String end, String datatime){
            SimpleDateFormat localTime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try{
                Date sdate = localTime.parse(star);
                Date edate=localTime.parse(end);
                Date date = localTime.parse(toLocalTime(datatime));
                System.out.println(sdate.getTime()+"##"+date.getTime()+"##"+edate.getTime());
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


    public static class getSimple2Reducer extends Reducer<Text, Text, Text, Text> {
        private Text macList = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line = null;
            for (Text tmp : values) {
                line += tmp.toString();
            }
            macList.set(line);
            context.write(key, macList);
        }
    }
}


