package wifipix.jcdecaux.cleandata;

import com.sun.xml.internal.fastinfoset.algorithm.BuiltInEncodingAlgorithm;
import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by aurora on 15/10/18.
 */
public class CleanData {
    public static class cleanDataMapper extends Mapper<Object, Text, Text, NullWritable> {
        private Configuration conf;
        // 先过滤AP MAC而后再过滤是否是手机Mac
        private Map<String, Boolean> isApMac = new HashMap<String, Boolean>();
        private Map<String, Boolean> isPhoneMac = new HashMap<String, Boolean>();
        private BufferedReader filterfile;
        private Text jcdecauxdata = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("Enter map setup function ");
            conf = context.getConfiguration();
            URI[] filterURIS = Job.getInstance(conf).getCacheFiles();
            for (URI tmpURI : filterURIS) {
                String filterFileName = new Path(tmpURI.getPath()).getName();
                System.out.println("get the all filter name is " + filterFileName);
                parseFilterMac(filterFileName);
            }
        }

        private void parseFilterMac(String fileName) {
            String tmp = null;
            try {
                filterfile = new BufferedReader(new FileReader(fileName));
                while ((tmp = filterfile.readLine()) != null) {
                    if (fileName.equals("isApMAC")) {
                        if ("mac".equals(tmp))
                            continue;
                        isApMac.put(tmp, true);
                    }

                    if (fileName.equals("isPhoneMac")) {
                        isPhoneMac.put(tmp, true);
                    }
                }
                //第二种方法：
//                for (Object key : isApMac.keySet()) {
//                    System.out.println("The isApMac key is " + key + " value is " + isApMac.get(key));
//                }
//                for (Object key : isPhoneMac.keySet()) {
//                    System.out.println("The isPhoneMac key is " + key + " value is " + isPhoneMac.get(key));
//                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file " + StringUtils.stringifyException(ioe));
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

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String formatline = value.toString();
            String[] field = formatline.split(" ");
            String inAPlist,inPhoneMac;
            // 先过滤AP MAC而后再过滤是否是手机Mac,isPhoneMac format xx:xx:xx
            // 原始数据格式可得到 AP采集到数据，及数据域不包含相关AP的最终数据
            if (isApMac.get(field[1]) != null) {
                if (isApMac.get(field[2]) == null) {
                    if (isPhoneMac.get(field[2].substring(0, 8)) != null) {
                        formatline = field[0] + " " + field[1] + " " + field[2];
                        jcdecauxdata.set(formatline);
                        context.write(jcdecauxdata, NullWritable.get());
                    }
                }

            }
        }
    }

    public static class cleanDataReducer extends Reducer<Text, NullWriter, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWriter> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
}
