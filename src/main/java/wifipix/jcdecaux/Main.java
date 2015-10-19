package wifipix.jcdecaux;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wifipix.jcdecaux.cleandata.CleanData;
import wifipix.jcdecaux.processone.ProcessOne;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by aurora on 15/10/17.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        System.out.println("-------------Begain run task----------");
        runJob(conf);

    }


    public static void runJob(Configuration conf) {
        try {
            Job cleanDataJob = createClenaDataJob(conf);
            Job processDataOneJob = createProcessDataOne(conf);
            Job processDataTwoJob = createProcessDataTwo(conf);
            Job processDataThreeJob = createProcessDataThree(conf);
            Job grepMacOneJob = createGrepMacOne(conf);
            Job grepMacTwoJob = createGrepMacTwo(conf);


            ControlledJob cleanDataCj = new ControlledJob(conf);
            cleanDataCj.setJob(cleanDataJob);
            ControlledJob processDataOneCj = new ControlledJob(conf);
            processDataOneCj.setJob(processDataOneJob);
            ControlledJob processDataTwoCj = new ControlledJob(conf);
            processDataTwoCj.setJob(processDataTwoJob);
            ControlledJob processDataThreeCj = new ControlledJob(conf);
            processDataThreeCj.setJob(processDataThreeJob);

            ControlledJob grepMacOneJobCj = new ControlledJob(conf);
            grepMacOneJobCj.setJob(grepMacOneJob);

            ControlledJob grepMacTwoJobCj = new ControlledJob(conf);
            grepMacTwoJobCj.setJob(grepMacTwoJob);

            // 设置串行子任务
            processDataOneCj.addDependingJob(cleanDataCj);
            processDataTwoCj.addDependingJob(cleanDataCj);
            processDataThreeCj.addDependingJob(cleanDataCj);
            grepMacOneJobCj.addDependingJob(cleanDataCj);
            grepMacTwoJobCj.addDependingJob(cleanDataCj);

            JobControl jobControl = new JobControl("JCDecaux");
            jobControl.addJob(cleanDataCj);
            jobControl.addJob(processDataOneCj);
            jobControl.addJob(processDataTwoCj);
            jobControl.addJob(processDataThreeCj);
            jobControl.addJob(grepMacOneJobCj);
            jobControl.addJob(grepMacTwoJobCj);

            int jobLength = jobControl.getWaitingJobList().size();
            Thread t = new Thread(jobControl);
            t.start();

            while (true) {
                if (jobControl.allFinished()) {
                    jobControl.stop();

                    System.out.println("=========allFinished=============");
                    break;
                }

                if (jobControl.getFailedJobList().size() > 0) {
                    jobControl.stop();
                    System.out.println("++++++++please to try+++++++");
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static Job createClenaDataJob(Configuration conf) throws Exception {
//        String input = "/jcdecaux/realdata";
        String input = "/wifipix/retail/daily/raw/2015/09/20150918";
        String formatOut = "/jcdecaux/formatdata";
        String filter = "/jcdecaux/filter";
        Path inputPath = new Path(input);
        Path formatOutPath = new Path(formatOut);
        boolean deleteOutputPath = formatOutPath.getFileSystem(conf).delete(formatOutPath, true);
        System.out.println("Delete OutputPath is " + deleteOutputPath);

        Path filterPath = new Path(filter);

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf,"Clean data");
        job.setJarByClass(Main.class);
        job.setMapperClass(wifipix.jcdecaux.cleandata.CleanData.cleanDataMapper.class);
        job.setCombinerClass(wifipix.jcdecaux.cleandata.CleanData.cleanDataReducer.class);
        job.setReducerClass(wifipix.jcdecaux.cleandata.CleanData.cleanDataReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 读入过滤文件，通过mac过滤出先关数据 ,此部分过滤数据只保存，AP采集到的手机MAC
        List<String> fns = addFile2cachFile(fs,filterPath,job);
        for (String tmp : fns) {
            System.out.println("get the fitler file name " + tmp);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, formatOutPath);

        return job;
    }

    public static Job createProcessDataOne(Configuration configuration) throws Exception {
        String input = "/jcdecaux/formatdata";
        String output = "/jcdecaux/processDataOne";

        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        boolean deleteOutputPath = outputPath.getFileSystem(configuration).delete(outputPath, true);
        System.out.println("Delete Process data one OutputPath is " + deleteOutputPath);

        Job job = Job.getInstance(configuration, "Process DATA one");
        job.setJarByClass(Main.class);
        job.setMapperClass(wifipix.jcdecaux.processone.ProcessOne.getSimple1Mapper.class);
        job.setCombinerClass(wifipix.jcdecaux.processone.ProcessOne.getSimple1Reducer.class);
        job.setReducerClass(wifipix.jcdecaux.processone.ProcessOne.getSimple1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }


    public static Job createProcessDataTwo(Configuration configuration) throws Exception {
        String input = "/jcdecaux/formatdata";
        String output = "/jcdecaux/processDataTwo";

        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        boolean deleteOutputPath = outputPath.getFileSystem(configuration).delete(outputPath, true);
        System.out.println("Delete Process data Two OutputPath is " + deleteOutputPath);

        Job job = Job.getInstance(configuration, "Process DATA Two");
        job.setJarByClass(Main.class);
        job.setMapperClass(wifipix.jcdecaux.processtwo.ProcessTwo.getSimple1Mapper.class);
        job.setCombinerClass(wifipix.jcdecaux.processtwo.ProcessTwo.getSimple1Reducer.class);
        job.setReducerClass(wifipix.jcdecaux.processtwo.ProcessTwo.getSimple1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }


    public static Job createProcessDataThree(Configuration configuration) throws Exception {
        String input = "/jcdecaux/formatdata";
        String output = "/jcdecaux/processDataThree";

        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        boolean deleteOutputPath = outputPath.getFileSystem(configuration).delete(outputPath, true);
        System.out.println("Delete Process data Three OutputPath is " + deleteOutputPath);

        Job job = Job.getInstance(configuration, "Process DATA Three");
        job.setJarByClass(Main.class);
        job.setMapperClass(wifipix.jcdecaux.processthree.ProcessThree.getSimple1Mapper.class);
        job.setCombinerClass(wifipix.jcdecaux.processthree.ProcessThree.getSimple1Reducer.class);
        job.setReducerClass(wifipix.jcdecaux.processthree.ProcessThree.getSimple1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }


    public static Job createGrepMacOne(Configuration configuration) throws Exception {
        String input = "/jcdecaux/formatdata";
        String output = "/jcdecaux/grepmacone";

        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        boolean deleteOutputPath = outputPath.getFileSystem(configuration).delete(outputPath, true);
        System.out.println("Delete grepmacone OutputPath is " + deleteOutputPath);

        Job job = Job.getInstance(configuration, "Job grepmacone");
        job.setJarByClass(Main.class);
        job.setMapperClass(wifipix.jcdecaux.grepmac.one.getSimple1Mapper.class);
        job.setCombinerClass(wifipix.jcdecaux.grepmac.one.getSimple1Reducer.class);
        job.setReducerClass(wifipix.jcdecaux.grepmac.one.getSimple1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    public static Job createGrepMacTwo(Configuration configuration) throws Exception {
        String input = "/jcdecaux/formatdata";
        String output = "/jcdecaux/grepmactwo";

        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        boolean deleteOutputPath = outputPath.getFileSystem(configuration).delete(outputPath, true);
        System.out.println("Delete grepmactwo OutputPath is " + deleteOutputPath);

        Job job = Job.getInstance(configuration, "Job grepmactwo");
        job.setJarByClass(Main.class);
        job.setMapperClass(wifipix.jcdecaux.grepmac.two.getSimple2Mapper.class);
        job.setCombinerClass(wifipix.jcdecaux.grepmac.two.getSimple2Reducer.class);
        job.setReducerClass(wifipix.jcdecaux.grepmac.two.getSimple2Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

        private static List<String> addFile2cachFile(FileSystem fs, Path input, Job job) throws IOException {
       List<String> filename = new ArrayList<String>();
       RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(input, true);
       while (fileStatusListIterator.hasNext()) {
           LocatedFileStatus fileStatus = fileStatusListIterator.next();
           job.addCacheFile(new Path(fileStatus.getPath().toString()).toUri());
           filename.add(fileStatus.getPath().getName());
       }
       return filename;
    }

    private static void addFile2InputPath(FileSystem fs,Path input,Job job) throws IOException {
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(input, true);
        while (fileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
            FileInputFormat.addInputPath(job, fileStatus.getPath().getParent());
        }
    }

}


