

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class CustomAnalyzer {

     void processor(String input, String output) throws Exception{
         BufferedReader bf = new BufferedReader(new FileReader(input));
         PrintStream ps = new PrintStream(new FileOutputStream(output));
         String line;
         while ((line = bf.readLine())!=null) {
            String eventID,description,extraction;
            eventID = line.substring(0,line.indexOf(","));
            description = line.substring(line.indexOf(",")+1);
            String str = "[`\\\\~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%…&*（）——+|{}【】‘；：”“’。，、？]";
            description = description.replaceAll(str," ");
            extraction = spliter(description);
            ps.println(eventID+" "+extraction);
         }
         bf.close();
    }

     String spliter(String str) throws Exception{
         Pattern pattern = Pattern.compile("[0-9]*");

        Analyzer analyzer=new StandardAnalyzer();
        TokenStream stream = analyzer.tokenStream("",new StringReader(str));

        stream = new PorterStemFilter(stream);
        stream.reset();
        CharTermAttribute cta=stream.addAttribute(CharTermAttribute.class);
        String output="";
        while (stream.incrementToken()){
            Matcher isNum = pattern.matcher(cta+"");
            if(isNum.matches()) {
                continue;
            }
            /*
            if(duplicate.contains(cta+"")) {
                continue;
            }
            */
            output=output+cta+" ";
            //duplicate.add(cta+"");
        }
        return output;
    }

    void tfidf(String input, String output) throws Exception{

        String line;
        BufferedReader bf = new BufferedReader(new FileReader(input));
        PrintStream ps = new PrintStream(new FileOutputStream(output));

        HashMap<String,Integer> occurrenceFileNum = new HashMap<>();
        int totalFileNum = 0;

        while ((line = bf.readLine())!=null) {

            HashSet<String> auxiliary=new HashSet<>();

            totalFileNum++;

            String[] contents = line.split(" ");
            for(int i=1;i<contents.length;i++){
                if (!auxiliary.contains(contents[i])){
                    auxiliary.add(contents[i]);
                    if(occurrenceFileNum.containsKey(contents[i])){
                        int tmp = occurrenceFileNum.get(contents[i])+1;
                        occurrenceFileNum.remove(contents[i]);
                        occurrenceFileNum.put(contents[i],tmp);
                    }
                    else {
                        occurrenceFileNum.put(contents[i],1);
                    }
                }
            }
        }
        bf.close();

        HashMap<String,Double> IDF = new HashMap<>();
        for(HashMap.Entry<String,Integer> entry : occurrenceFileNum.entrySet()){
            String key = entry.getKey();
            int value = entry.getValue();
            IDF.put(key,Math.log(totalFileNum/(double)value));
        }

        bf = new BufferedReader(new FileReader(input));
        while ((line = bf.readLine())!=null) {

            String[] contents = line.split(" ");
            String eventID = contents[0];

            ArrayList<WordPair> rank=new ArrayList<>();

            int total = contents.length-1;

            HashMap<String,Integer> counts = new HashMap<>();
            for (int i=1;i<contents.length;i++){
                if (counts.containsKey(contents[i])){
                    int tmp = counts.get(contents[i])+1;
                    counts.remove(contents[i]);
                    counts.put(contents[i],tmp);
                }
                else {
                    counts.put(contents[i],1);
                }
            }

            for(HashMap.Entry<String,Integer> entry : counts.entrySet()){

                String key = entry.getKey();
                int count = entry.getValue();

                double tf = count/(double)total;
                double idf = IDF.get(key);
                double value = tf*idf;

                rank.add(new WordPair(key,value));
            }

            WordPairScoreComparator comparator = new WordPairScoreComparator();
            rank.sort(comparator);
            String tmp = "";
            for(int i=0;i<10 && i<rank.size();i++){
                if(rank.get(i).score==0){
                    break;
                }
                tmp+=(rank.get(i).content+" ");
            }

            ps.println(eventID+" "+tmp);
        }
        bf.close();
    }

    class WordPair{
        String content;
        double score;

        WordPair(String content,double score){
            this.content=content;
            this.score=score;
        }
    }

    class WordPairScoreComparator implements Comparator<WordPair> {
        @Override
        public int compare(WordPair x, WordPair y) {
            if (x.score < y.score) {
                return 1;
            } else if (x.score > y.score) {
                return -1;
            }
            return 0;
        }
    }
}
