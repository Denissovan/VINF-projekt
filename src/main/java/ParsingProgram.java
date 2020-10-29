import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class ParsingProgram {


    public static String getAtributeFromLink(String link){
        if(link.matches("[<>].*[<>].*")) {
            String localLink = "";
            String[] localStrs;
            localLink = link;
            localLink = localLink.replace("<", "");
            localLink = localLink.replace(">", "");
            localStrs = localLink.split("/");
            return localStrs[localStrs.length - 1];
        }else {
            return link;
        }
    }


    public static String removeDuplicates(String data){

        String[] strs = data.split("\\|");

        Set<String> set = new HashSet<String>(Arrays.asList(strs));
        List<String> listSet = new ArrayList<String>(set);

        Collections.sort(listSet);

        String returnStr = "";

        //System.out.println("//////////////// Zacina volanie funkcie removeDuplicates //////////////");

        for (String s : listSet){
            returnStr += s + "|";
        }

        //System.out.println("///////////////// Skoncilo volanie funkcie removeDuplicates ///////////////");
        return returnStr;
    }
    public static String removeUnnecessary(String data){
        String[] strs = data.split("\\|");
        List<String> list = new ArrayList<String>(Arrays.asList(strs));
        String returnStr = "";

        //System.out.println("//////////////// Zacina volanie funkcie removeDuplicates //////////////");

        Pattern p1 = Pattern.compile(".*((22-rdf-syntax-ns#type)|(base\\.type_ontology)).*");


        for (String s : list){
            Matcher m1 = p1.matcher(s);
            if(!m1.matches()){
                returnStr += s + "|";
            }
        }

        //System.out.println("///////////////// Skoncilo volanie funkcie removeDuplicates ///////////////");
        return returnStr;
    }

    public static boolean containsID(String idToFind, File file) throws IOException {

        BufferedReader buff = new BufferedReader(new FileReader(file));

        String fileLine = null;
        String idInFile = null;

        while((fileLine = buff.readLine()) != null){

            idInFile = fileLine.split("\t")[0];

            if(idToFind.equals(idInFile)){
                return true;

            }
        }
        return false;
    }

    public static class MovieIdMapper extends Mapper<Object , Text, Text, IntWritable> {


        IntWritable one = new IntWritable(1);
        private final Text Key = new Text();
        String line = null;
        String lineID = null;
        String[] lineParts;
        String middlePart = null;

        public void map(Object key, Text Document, Context context) throws IOException, InterruptedException {

            StringTokenizer documentLine = new StringTokenizer(Document.toString(), "\n", false);



            // read line by line
            if (documentLine.hasMoreTokens()) {
                line = documentLine.nextToken();

                lineParts = line.split("\t");
                lineID = lineParts[0];
                middlePart = lineParts[1];

                Pattern p1 = Pattern.compile(".*((ns/film\\.film\\.)|(tv\\.tv_program\\.)).*");
                Matcher m1 = p1.matcher(middlePart);


                if (m1.matches() ) {
                    Key.set(getAtributeFromLink(lineID));
                    context.write(Key, one);
                }

            }
        }
    }

    public static class MovieIdReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static class MovieFindMapper extends Mapper<Object , Text, Text, Text>{



        private final Text value = new Text();
        String line = null;
        String lineID = null;
        String movieID = null;
        String[] lineComponents;
        boolean idInFile = false;


        public void map(Object key, Text Document, Context context) throws IOException, InterruptedException {

            StringTokenizer documentLine = new StringTokenizer(Document.toString(), "\n", false);

            Configuration conf = context.getConfiguration();
            File fID = new File(conf.get("IDs"));


            // read line by line
            if(documentLine.hasMoreTokens()){
                line = documentLine.nextToken();
                lineComponents = line.split("\t");
                lineID = getAtributeFromLink(lineComponents[0]);


                if(!idInFile) {
                    if (containsID(lineID, fID)) {
                        movieID = lineID;
                    }
                }

                if(lineID.equals(movieID)){
                    Text id = new Text();
                    value.set(getAtributeFromLink(lineComponents[1]) + ":" + lineComponents[2]);
                    id.set(lineID);
                    context.write(id,value);
                    idInFile = true;
                }
                else {
                    idInFile = false;
                }

            }


        }
    }

    public static class MovieFindReducer extends Reducer<Text,Text,Text,Text> {

        Text textValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String  txt = "";
            //parse and create while tke groupKey matches the local key a.k.a lineID
            for(Text val : values){

                txt = txt + " " + val;
                txt += "|";
            }
            //manage duplicates
            txt = removeDuplicates(txt);
            txt = removeUnnecessary(txt);

            txt = "=[" + txt + "]";

            textValue.set(txt);
            context.write(key, textValue);
        }
    }

    public static class MovieFilterMapper extends Mapper<Object , Text, Text, Text>{



        private final Text value = new Text();
        String line = null;
        String lineID = null;
        String movieID = null;
        String[] lineComponents;
        boolean idInFile = false;


        public void map(Object key, Text Document, Context context) throws IOException, InterruptedException {

            StringTokenizer documentLine = new StringTokenizer(Document.toString(), "\n", false);

            Configuration conf = context.getConfiguration();
            File fID = new File(conf.get("UnfilteredObjects"));


            // read line by line
            if(documentLine.hasMoreTokens()){
                line = documentLine.nextToken();


                System.out.println(line);
            }


        }
    }
    public static class MovieFilterReducer extends Reducer<Text,Text,Text,Text> {

        Text textValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String  txt = "";
            //parse and create while tke groupKey matches the local key a.k.a lineID
            for(Text val : values){

                txt = txt + " " + val;
                txt += "|";
            }
            //manage duplicates
            txt = removeDuplicates(txt);

            txt = "\n[" + txt + "]\n";

            textValue.set(txt);
            context.write(key, textValue);
        }
    }

    /* first argument args[0] is the file path of the input data
       second argument args[1] is the file path of the Movie id output data
       third argument args[2] is the file path of the unfiltered Movie objects
       fourth argument args[3] is the file path of the filtered Movie objects
    */
    public static void main(String[] args) throws Exception {


        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "id parser");
        job1.setJarByClass(ParsingProgram.class);
        job1.setMapperClass(MovieIdMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(MovieIdReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        //System.exit(job1.waitForCompletion(true) ? 0 : 1);
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("IDs", args[2]);

        Job job2 = Job.getInstance(conf2, "movie/tv_program finder");
        job2.setJarByClass(ParsingProgram.class);
        job2.setMapperClass(MovieFindMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(MovieFindReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        //System.exit(job2.waitForCompletion(true) ? 0 : 1);
        job2.waitForCompletion(true);

        Configuration conf3 = new Configuration();
        conf3.set("UnfilteredObjects", args[4]);

        Job job3 = Job.getInstance(conf3, "object filter");
        job3.setJarByClass(ParsingProgram.class);
        job3.setMapperClass(MovieFilterMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        //job3.setReducerClass(MovieFilterReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[4]));
        FileOutputFormat.setOutputPath(job3, new Path(args[5]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);

    }

}
