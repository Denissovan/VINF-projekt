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


    public static HashMap <String, String> entityLinksMap = new HashMap<>();


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
    public static String parseIDs(String id){
        return  id.replaceAll("\\+", ":");
    }
    public static String parseNames(String name){
        String[] nameParts = name.split("@");

        return  nameParts[0].split("\\+")[1];
    }
    public static String parseAliases(String alias){
        String[] aliasParts = alias.split("\\+");

        return  aliasParts[1];
    }
    public static String parseDirectedBys(String directedBy){
        directedBy = directedBy.replaceAll(">", "");
        String[] directedByParts = directedBy.split("/");
        return  directedByParts[directedByParts.length-1];
    }
    public static String parseGenres(String genre){
        genre = genre.replaceAll(">", "");
        String[] genreParts = genre.split("/");
        return  genreParts[genreParts.length-1];
    }
    public static String parseDescription(String description){
        String[] descriptionParts = description.split("@en");

        return  descriptionParts[0].split("\\+")[1];
    }
    public static String parseTvOrFilm(String tv_or_film){
        tv_or_film = tv_or_film.replaceAll(">", "");
        String[] tv_filmParts = tv_or_film.split("/");
        return  tv_filmParts[tv_filmParts.length-1];
    }
    public static String parseReleasDate(String release_date){

        String[] dateParts = release_date.split("\\^\\^");
        //String date = dateParts[0].split(":")[1];

        return  dateParts[0].split("\\+")[1];
    }

    public static String parseNotableObj(String notableObj){
        String[] notableParts = notableObj.split("\\+");
        return getAtributeFromLink(notableParts[notableParts.length-1]);
    }
    public static String parseDisplayName(String displayName){
        String[] displayNameParts = displayName.split("\\+");
        return displayNameParts[displayNameParts.length-1];
    }


    public static String getRelevantAtributes(String data){
        String[] strs = data.split("\\|");
        List<String> list = new ArrayList<String>(Arrays.asList(strs));
        List<String> objectNames = new ArrayList<>();
        List<String> aliases = new ArrayList<>();
        String tvOrFilm = "None";
        List<String> directedBy = new ArrayList<>();
        String releaseDate = "None";
        String description = "None";
        List<String> genres = new ArrayList<>();
        String delim = ",";
        String returnStr;


        Pattern objectName_pat = Pattern.compile(".*(type\\.object\\.name).*(@en).*");

        Pattern alias_pat = Pattern.compile(".*(common\\.topic\\.alias).*");

        Pattern tv_programOrFilm_pat = Pattern.compile(".*(type\\.object\\.type:<http://rdf\\.freebase\\.com/ns/" +
                "(tv\\.tv_program>)|(film\\.film)).*");

        Pattern directedBy_pat = Pattern.compile(".*(film\\.film\\.directed_by).*");

        Pattern genre_pat = Pattern.compile(".*(((tv\\.tv_program)|( film\\.film))\\.genre).*");

        Pattern description_pat = Pattern.compile(".*(common\\.topic\\.description).*(@en).*");

        Pattern releaseDate_pat = Pattern.compile(".*((film\\.film\\.initial_release_date)|(tv.tv_program.air_date_of_first_episode)).*");


        for (String s : list){
            Matcher objectName_match = objectName_pat.matcher(s);
            Matcher alias_match = alias_pat.matcher(s);
            Matcher tv_programOrFilm_match = tv_programOrFilm_pat.matcher(s);
            Matcher directedBy_match = directedBy_pat.matcher(s);
            Matcher genre_match = genre_pat.matcher(s);
            Matcher description_match = description_pat.matcher(s);
            Matcher releaseDate_match = releaseDate_pat.matcher(s);


            if(objectName_match.matches()){
                objectNames.add(parseNames(s));
            }
            if(alias_match.matches()){
                aliases.add(parseAliases(s));
            }
            if(tv_programOrFilm_match.matches()){
                tvOrFilm = parseTvOrFilm(s);
            }
            if(directedBy_match.matches()){
                directedBy.add(parseDirectedBys(s));
            }
            if(genre_match.matches()){
                genres.add(parseGenres(s));
            }
            if(description_match.matches()){
                description = parseDescription(s);
            }
            if(releaseDate_match.matches()){
                releaseDate = parseReleasDate(s);
            }
        }

        String ob_names = "name{" + (objectNames.isEmpty() ? "None" : String.join(delim, objectNames)) + "}";
        String al = "aliases{" + (aliases.isEmpty() ?  "None" : String.join(delim, aliases)) + "}";
        String dir_by = "directed_by{" + (directedBy.isEmpty() ? "None" : String.join(delim, directedBy)) + "}";
        String gens = "genres{" + (genres.isEmpty() ? "None" : String.join(delim, genres)) + "}";

        description = "description{" + description + "}";
        tvOrFilm = "tv_or_film{" + tvOrFilm + "}";
        releaseDate = "release_date{" + releaseDate + "}";


        returnStr = ob_names + "|" + al + "|" + dir_by + "|" + gens +
                "|" + description + "|"+
                tvOrFilm + "|" + releaseDate;


        return returnStr;
    }

    public static String getGeneralAtributes(String data){

        String[] strs = data.split("\\|");
        List<String> list = new ArrayList<String>(Arrays.asList(strs));
        String objectLink = "None";
        String objectValue = "None";

        Pattern objLinkPat = Pattern.compile(".*(common\\.notable_for\\.notable_object).*");
        Pattern objValuePat = Pattern.compile(".*(common\\.notable_for\\.display_name).*(@en).*");


        for (String s : list){
            Matcher objLink_match = objLinkPat.matcher(s);
            Matcher objValue_match = objValuePat.matcher(s);

            if (objLink_match.matches()){
                objectLink = parseNotableObj(s);
            }
            if (objValue_match.matches()){
                objectValue = parseDisplayName(s);
            }
        }

        return objectLink + "|" + objectValue;
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

    public static  int containsEntity(String text){
        String[] strs = text.split("\n");
        List<String> list = new ArrayList<String>(Arrays.asList(strs));


        Pattern namePat = Pattern.compile(".*(type\\.object\\.name).*(@en).*");
        Pattern genrePat = Pattern.compile(".*((/tv/tv_program)|(/film/film)/genre).*");


        for (String s : list) {
            Matcher m1 = namePat.matcher(s);
            Matcher m2 = genrePat.matcher(s);
            if (m1.matches()) {
                return 1;  // name matched
            }
            else if (m2.matches()){
                return 2; // genre matched
            }
        }
        return 0;
    }


    public static class MovieIdMapper extends Mapper<Object , Text, Text, IntWritable> {


        IntWritable one = new IntWritable(1);
        private final Text Key = new Text();
        String line = null;
        String lineID = null;
        String[] lineParts;
        String middlePart = null;
        String lastPart = null;

        public void map(Object key, Text Document, Context context) throws IOException, InterruptedException {

            StringTokenizer documentLine = new StringTokenizer(Document.toString(), "\n", false);



            // read line by line
            if (documentLine.hasMoreTokens()) {
                line = documentLine.nextToken();

                lineParts = line.split("\t");
                lineID = lineParts[0];
                middlePart = lineParts[1];
                lastPart = lineParts[2];

                Pattern p1 = Pattern.compile(".*((ns/film\\.film\\.)|(tv\\.tv_program\\.)).*");
                Matcher m1 = p1.matcher(middlePart);

                Pattern p2 = Pattern.compile(".*((/tv/tv_program)|(/film/film)/genre).*");
                Matcher m2 = p2.matcher(lastPart);

                if (m1.matches() || m2.matches()) {
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
        String baseID = null;
        String[] lineComponents;
        boolean equals = false;
        boolean isMovie = false;
        boolean firstToSet = true;


        public void map(Object key, Text Document, Context context) throws IOException, InterruptedException {

            StringTokenizer documentLine = new StringTokenizer(Document.toString(), "\n", false);

            Configuration conf = context.getConfiguration();
            File fID = new File(conf.get("IDs"));


            // read line by line
            if(documentLine.hasMoreTokens()){
                line = documentLine.nextToken();
                lineComponents = line.split("\t");
                lineID = getAtributeFromLink(lineComponents[0]);




                if(!equals && !isMovie) {
                    isMovie = containsID(lineID, fID); // if it contains true is asigned else false
                }

                if(firstToSet){
                    baseID = lineID;
                    firstToSet = false;
                }
/*
                if(lineID.contains("g.122r7f3r")){
                    System.out.println("Tu sa to zacina");
                }
*/
                if(lineID.equals(baseID)){
                    if(isMovie) {
                        Text id = new Text();
                        // mam problem ze zapisujem o jeden dopredu a stracam ten prvy , treba si zapametat
                        // predchadzajuci ...
                        value.set(getAtributeFromLink(lineComponents[1]) + "+" + lineComponents[2]);
                        id.set(lineID);
                        context.write(id, value);
                    }
                    equals = true;
                }
                else {
                    //firstToSet = true;
                    equals = false;
                    isMovie = containsID(lineID, fID);
                    baseID = lineID;
                    if(isMovie){
                        Text id = new Text();
                        value.set(getAtributeFromLink(lineComponents[1]) + "+" + lineComponents[2]);
                        id.set(lineID);
                        context.write(id, value);
                    }
                    //isMovie = false;
                }

            }


        }
    }

    public static class MovieFindReducer extends Reducer<Text,Text,Text,Text> {

        Text textValue = new Text();
        Text keyValue = new Text();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String  txt = "";

            //parse and create while tke groupKey matches the local key a.k.a lineID
            for(Text val : values){

                txt = txt + " " + val;
                txt += "|";
            }

            int entity = containsEntity(txt);



            switch (entity) {
                case 1:   //film / tv_program

                    txt = removeDuplicates(txt);
                    txt = getRelevantAtributes(txt);

                    txt = "-||->[" + txt + "]";

                    textValue.set(txt);
                    context.write(key, textValue);

                    break;
                case 2: // tv/fil genre
                    System.out.println("Dostanem sa sem");

                    // tu sa to niekde pokazi
                    //System.out.println(key.toString() + "**->" + txt);
                    //System.out.println(txt);

                    //System.out.println(txt);

                    txt = removeDuplicates(txt);
                    txt = getGeneralAtributes(txt); // it can be used not only for genres ...


                    String[] dataParts = txt.split("\\|");


                    //System.out.println(dataParts[0] + "-" + dataParts[1]);

                    if ((!dataParts[0].equals("None") || !dataParts[1].equals("None")) && !entityLinksMap.containsKey(dataParts[0])){
                        entityLinksMap.put(dataParts[0], dataParts[1]);
                    }
                    System.out.println("Dostanem sa aj tam");
                    break;
            }



        }
    }

    public static class MovieFilterMapper extends Mapper<Object , Text, Text, Text>{



        private final Text value = new Text();
        private final Text Key = new Text();
        String line = null;
        String lineID = null;
        String movieID = null;
        String[] lineComponents;
        boolean idInFile = false;


        public void map(Object key, Text Document, Context context) throws IOException, InterruptedException {

            StringTokenizer documentLine = new StringTokenizer(Document.toString(), "\n", false);

            //Configuration conf = context.getConfiguration();
            //File fID = new File(conf.get("UnfilteredObjects"));


            // read line by line
            if(documentLine.hasMoreTokens()){
                line = documentLine.nextToken();
                lineComponents = line.split("-\\|\\|->");
                Key.set(lineComponents[0]);
                value.set(lineComponents[1]);

                context.write(Key, value);
            }

        }
    }
    public static class MovieFilterReducer extends Reducer<Text,Text,Text,Text> {

        Text textValue = new Text();
        boolean dah = false;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String  txt = "";
            //parse and create while tke groupKey matches the local key a.k.a lineID
            for(Text val : values){

                txt = txt + " " + val;
                txt += "|";
            }

            Pattern namePat = Pattern.compile(".*(name).*");
            Matcher name_match = namePat.matcher(txt);


            if(name_match.matches()) {

                String[] txtParts = txt.split("\\|");
                String newText = "";
                String toReplace = "";

                String[] toReplaceParts;
                String[] links;


                for(String s : txtParts){
                    if(s.matches(".*genres.*")){
                        toReplace = s.replaceAll("[{}]", "|");
                        toReplaceParts = toReplace.split("\\|");
                        links = toReplaceParts[1].split(",");
                        List<String> list = new ArrayList<>();
                        for(String l : links){
                            list.add(entityLinksMap.get(l));
                        }

                        toReplaceParts[1] = String.join(",", list);

                        s = toReplaceParts[0] + "{" + toReplaceParts[1] + "}";
                    }
                    newText = newText + " " + s;
                    newText += "|";
                }

                textValue.set(newText);
                context.write(key, textValue);
            }

            if(!dah){
                for(String s : entityLinksMap.keySet()){
                    System.out.println(s + " " + entityLinksMap.get(s));
                }
                dah = true;
            }


        }
    }

    /* first argument args[0] is the file path of the input data
       second argument args[1] is the file path of the Movie id output data
       third argument args[2] is the file path of the unfiltered Movie objects
       fourth argument args[3] is the file path of the filtered Movie objects
    */
    public static void main(String[] args) throws Exception {


        System.out.println("-----------Starting Job1 -----------");

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

        System.out.println("-----------Job1 Completed-----------");

        Configuration conf2 = new Configuration();
        conf2.set("IDs", args[2]);

        System.out.println("-----------Starting Job2 -----------");

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

        System.out.println("-----------Job2 Completed -----------");

        Configuration conf3 = new Configuration();
        //conf3.set("UnfilteredObjects", args[4]);

        System.out.println("-----------Starting Job3 -----------");

        Job job3 = Job.getInstance(conf3, "object filter");
        job3.setJarByClass(ParsingProgram.class);
        job3.setMapperClass(MovieFilterMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job3.setReducerClass(MovieFilterReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[4]));
        FileOutputFormat.setOutputPath(job3, new Path(args[5]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);

        System.out.println("-----------Job3 Completed-----------");

    }

}
