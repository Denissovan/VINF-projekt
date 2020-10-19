import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
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
            returnStr += s + "\n";
        }

        //System.out.println("///////////////// Skoncilo volanie funkcie removeDuplicates ///////////////");
        return returnStr;
    }


    public static class TokenizerMapper extends Mapper<Object , Text, Text, Text>{



        private final Text value = new Text();
        public ArrayList<String> buff = new ArrayList<String>(20);
        boolean upperAreComplete = false;
        String line = null;
        String lineID = null;

        public void map(Object key, Text Document, Context context) throws IOException, InterruptedException {

            StringTokenizer documentLine = new StringTokenizer(Document.toString(), "\n", false);

            // read line by line
            if(documentLine.hasMoreTokens()){
                line = documentLine.nextToken();
                line = line.replace("^", "\t");

                if (buff.size() == 20) {
                    buff.remove(0);
                }
                buff.add(line);



                // treba este stale nejak vymysliet aby mi grepol len relevantne filmy a tv programy
                if (line.matches(".*http://rdf\\.freebase\\.com/ns/imdb\\.topic\\.title_id.*") /*||
                        line.contains("http://rdf.freebase.com/ns/tv.tv_program.air_date")||
                        line.contains("http://rdf.freebase.com/ns/tv.tv_program.languages")||
                        line.contains("http://rdf.freebase.com/ns/film.film.country") ||
                        line.contains("http://rdf.freebase.com/ns/film.film.directed_by") ||
                        line.contains("http://rdf.freebase.com/ns/film.film.genre")*/) {


                    lineID = line.split("\t")[0]; // get the id

                    //process atributes from upper to below
                    for (String s : buff) {

                        if(s.contains(lineID)){

                            String[] arrLine = s.split("\t");
                            Text txt = new Text();
                            /* a function is needed that strips the links and creates json objects
                            and also deletes duplicates
                             */

                            txt.set(arrLine[0]);
                            //value.set(getAtributeFromLink(arrLine[1]) + " : " +
                            //        getAtributeFromLink(arrLine[2]) + ",");
                            value.set(arrLine[1] + " : " + arrLine[2] + ",");

                            context.write(txt, value);
                        }

                    }
                    upperAreComplete = true;

                }
                else{

                    if(upperAreComplete){
                        String[] localLine = line.split("\t");
                        String localLineId = localLine[0];
                            if (localLineId.contains(lineID)) {
                                Text txt = new Text();
                                txt.set(localLine[0]);
                                //value.set(getAtributeFromLink(localLine[1]) + " : " +
                                //        getAtributeFromLink(localLine[2])+ ",");
                                value.set(localLine[1] + " : " + localLine[2]+ ",");

                                context.write(txt, value);
                            }
                            else{
                                upperAreComplete = false;
                            }
                    }
                }

            }


        }
    }

    public static class ObjectReducer extends Reducer<Text,Text,Text,Text> {

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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "object parser");
        job.setJarByClass(ParsingProgram.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(ObjectReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
