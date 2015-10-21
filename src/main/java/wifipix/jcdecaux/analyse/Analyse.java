package wifipix.jcdecaux.analyse;

// intention
// (1) 9.18 早8:00~9:30，先被‘e4:95:6e:4f:54:95’侦测，之后又被其它任何一个探针侦测到的手机mac地址数。
// (2) 先被其它任何一个探针侦测到，之后又被‘54：95’的手机mac地址数。

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class Analyse {
    public static class analyseMapper extends Mapper<Object, Text, Text, LongWritable> {
        private Text userMac = new Text();
        private LongWritable userTime = new LongWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] field = line.split(" ");
            String theSpecialAP = "e4:95:6e:4f:54:95";
            String timeStart800 = "2015-09-18 08:00:00";
            String timeend935 = "2015-09-18 09:30:00";

            if (iswantTime(timeStart800, timeend935, field[0])) {
//                if (field[1].equals(theSpecialAP)) {
                    // dataformat  [ usermac_apmac time ]
                    userMac.set(field[2] + "_" + field[1]);
                    userTime.set(Long.parseLong(field[0]));
                    //给出userMac 和 userTime,  reduce统计出探针抓取到的相关mac的最后时间
                    context.write(userMac, userTime);
//                }
            }
        }

        public static boolean iswantTime(String star,String end, String datatime){
            SimpleDateFormat localTime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try{
                Date sdate = localTime.parse(star);
                Date edate=localTime.parse(end);
                Date date = localTime.parse(toLocalTime(datatime));
//                System.out.println(sdate.getTime()+"##"+date.getTime()+"##"+edate.getTime());
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

    public static class analyeReducer extends Reducer<Text, LongWritable, NullWritable, Text> {
        private Text allTimeData = new Text();
        private Text alldata = new Text();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            String sAllData = null;
            String[] field = key.toString().split("_");
            Long maxTime = Long.MIN_VALUE;
            Long minTime = Long.MAX_VALUE;

            // maxTime 最后扫到的时间  minTime最后扫到的时间
            for (LongWritable tmp : values) {
                long tmpvalues = tmp.get();
                maxTime = Math.max(maxTime, tmpvalues);
                minTime = Math.min(minTime, tmpvalues);
            }
            //todo: 查找连接字符串 高性能做法
            // 第一次访问在前 最后一次访问在后
            // dataformat [user]


            sAllData = field[0] + " " + field[1] + " " + minTime.toString() + " " + maxTime.toString();
            allTimeData.set(sAllData);
//            alldata.set("alldata");
            context.write(NullWritable.get(), allTimeData);
        }
    }
}
