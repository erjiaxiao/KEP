import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class Main {
    public static void main(String[] args) throws Exception {

        /*
        CustomAnalyzer customAnalyzer = new CustomAnalyzer();
        customAnalyzer.processor("act.txt","act_processed.txt");

        customAnalyzer.tfidf("act_processed.txt","act_processed_top.txt");
        */

        /*
        int[] kTuning = {1};

        for(int i=0;i<kTuning.length;i++){

            int count = 0;
            int k = kTuning[i];

            while (count<100) {
                TestingDataGenerator testingDataGenerator = new TestingDataGenerator();
                testingDataGenerator.generateTestingDataset("act_processed.txt", "mem.txt", "mem_training.txt", "mem_testing.txt", count);

                KEP kep = new KEP("act_processed.txt", "latlon.txt", "mem_training.txt");
                kep.FSRJFNLJAccuracyWithII(k);

                count++;
            }
        }
        */

        /*
        int count = 0;
        while (count<100) {
            TestingDataGenerator testingDataGenerator = new TestingDataGenerator();
            testingDataGenerator.generateTestingDataset("act_processed.txt", "mem.txt", "mem_training.txt", "mem_testing.txt", count);

            KEP kep = new KEP("act_processed.txt", "latlon.txt", "mem_training.txt");
            // kep.KeywordAccuracy();
            kep.KeywordAccuracyVer2();

            count++;
        }
        */

        /*
        int filenum = 1;
        int[] kTuning = {1, 5, 10, 15, 20};
        while (filenum<=5) {

            System.out.println("--------------------"+ filenum +".out----------------------");

            String line;
            BufferedReader bf = new BufferedReader(new FileReader(filenum + ".out"));

            ArrayList<Integer> FNLJStarRank = new ArrayList<>();
            ArrayList<Integer> FSRJStarRank = new ArrayList<>();
            while ((line = bf.readLine()) != null) {
                if (line.contains("FSRJrank")) {
                    String[] tmp = line.split(":");
                    FSRJStarRank.add(Integer.parseInt(tmp[tmp.length - 1]));
                } else if (line.contains("FNLJrank")) {
                    String[] tmp = line.split(":");
                    FNLJStarRank.add(Integer.parseInt(tmp[tmp.length - 1]));
                }
                if (line.contains("CASE Start: top-k = 5")) {
                    break;
                }
            }

            for (int i = 0; i < kTuning.length; i++) {
                int k = kTuning[i];
                int FNLJhit = 0;
                int FSRJhit = 0;
                for (int j = 0; j < FNLJStarRank.size(); j++) {
                    if (FNLJStarRank.get(j) < k) {
                        FNLJhit++;
                    }
                }
                for (int j = 0; j < FSRJStarRank.size(); j++) {
                    if (FSRJStarRank.get(j) < k) {
                        FSRJhit++;
                    }
                }

                System.out.println("k=" + k + " FNLJStar:" + FNLJhit / (float) FNLJStarRank.size() + " FSRJStar:" + FSRJhit / (float) FSRJStarRank.size() + "\n");
            }
            filenum++;
        }
        */

        /*
        int filenum = 1;
        int[] kTuning = {1, 5, 10, 15, 20};
        while (filenum<=5) {

            System.out.println("--------------------"+ filenum +".out----------------------");

            String line;
            BufferedReader bf = new BufferedReader(new FileReader(filenum + ".out"));

            ArrayList<Integer> KeywordRank = new ArrayList<>();
            while ((line = bf.readLine()) != null) {
                if (line.contains("Keywordrank")) {
                    String[] tmp = line.split(":");
                    KeywordRank.add(Integer.parseInt(tmp[tmp.length - 1]));
                }
            }

            for (int i = 0; i < kTuning.length; i++) {
                int k = kTuning[i];
                int Keywordhit = 0;
                for (int j = 0; j < KeywordRank.size(); j++) {
                    if (KeywordRank.get(j) < k) {
                        Keywordhit++;
                    }
                }

                System.out.println("k=" + k + " Keyword:" + Keywordhit / (float) KeywordRank.size() + "\n");
            }
            filenum++;
        }
        */

        /*
        EfficiencyTest("FNLJ","default");
        EfficiencyTest("FNLJ1","default");
        EfficiencyTest("FNLJ2","default");
        EfficiencyTest("FNLJ3","default");
        EfficiencyTest("FNLJStar","default");

        EfficiencyTest("FRJ","default");
        EfficiencyTest("FRJ1","default");
        EfficiencyTest("FRJ2","default");
        EfficiencyTest("FRJ3","default");
        EfficiencyTest("FRJStar","default");

        EfficiencyTest("FNLJ","k");
        EfficiencyTest("FNLJ","alpha");
        EfficiencyTest("FNLJ","keyword");

        EfficiencyTest("FNLJStar","k");
        EfficiencyTest("FNLJStar","alpha");
        EfficiencyTest("FNLJStar","keyword");

        EfficiencyTest("FRJStar","k");
        EfficiencyTest("FRJStar","alpha");
        EfficiencyTest("FRJStar","keyword");
        */

        /*
        generateQuerySet4CaseStudy("python","learning");
        caseStudy("FNLJStar");
        caseStudy("OriNLJ");
        selectCase();
        */
    }

    static void EfficiencyTest(String method,String parameter) throws Exception{

        int[] kTuning = {10,10,10,10,10};
        int[] keywordTuning = {3,3,3,3,3};
        double[] alphaTuning = {0.2,0.2,0.2,0.2,0.2};
        int[] rTuning = {1,1,1,1,1};

        if(parameter.equals("k")){
            int[] tmp = {1,5,10,15,20};
            kTuning = tmp;
        }
        else if(parameter.equals("keyword")){
            int[] tmp = {1,2,3,4,5};
            keywordTuning = tmp;
        }
        else if(parameter.equals("alpha")){
            double[] tmp = {0.1,0.3,0.5,0.7,0.9};
            alphaTuning = tmp;
        }
        else if(parameter.equals("r")){
            int[] tmp = {1,5,10,20,100};
            rTuning = tmp;
        }
        else if(parameter.equals("default")){
            int[] tmp1 = {10};
            int[] tmp2 = {3};
            double[] tmp3 = {0.2};
            int[] tmp4 = {1};
            kTuning = tmp1;
            keywordTuning = tmp2;
            alphaTuning = tmp3;
            rTuning = tmp4;
        }
        else {
            System.out.println("parameter error.");
            return;
        }

        ArrayList<Double> c = new ArrayList<>();
        ArrayList<Double> ep = new ArrayList<>();
        ArrayList<Double> ec = new ArrayList<>();
        ArrayList<Double> uc = new ArrayList<>();

        ArrayList<Double> ep2 = new ArrayList<>();

        for(int i=0;i<kTuning.length;i++) {

            int k = kTuning[i];
            int keywordNum = keywordTuning[i];
            int r = rTuning[i];
            double alpha = alphaTuning[i];

            int max_count = 5;
            int count = 0;

            double round = 0;
            double cost = 0;
            double eventNumPruning = 0;
            double eventNumInvolved = 0;
            double userNumInvolved = 0;
            double eventNumPruningByOpt1 = 0;


            while (count < max_count) {

                TestingDataGenerator testingDataGenerator = new TestingDataGenerator();
                testingDataGenerator.generateTestingDataset("act_processed.txt", "mem.txt", "mem_training.txt", "mem_testing.txt", count);
                testingDataGenerator.generateQueryset2("mem_testing.txt", "act_processed.txt", "latlon.txt", "query_testing.txt", count,keywordNum);

                String query;
                KEP kep = new KEP("act_processed.txt", "latlon.txt", "mem_training.txt");
                BufferedReader bf = new BufferedReader(new FileReader("query_testing.txt"));

                Long start = System.currentTimeMillis();
                while ((query = bf.readLine()) != null) {
                    if(method.equals("FNLJ")){
                        kep.FNLJ(query,k,alpha);
                    }
                    else if(method.equals("FNLJ1")){
                        kep.FNLJ1(query,k,alpha);
                    }
                    else if(method.equals("FNLJ2")){
                        kep.FNLJ2(query,k,alpha);
                    }
                    else if(method.equals("FNLJ3")){
                        kep.FNLJ3(query,k,alpha);
                    }
                    else if(method.equals("FNLJStar")){
                        kep.FNLJStar(query,k,alpha);
                    }
                    else if(method.equals("FRJ")){
                        kep.FRJ(query,k,alpha,r);
                    }
                    else if(method.equals("FRJ1")){
                        kep.FRJ1(query,k,alpha,r);
                    }
                    else if(method.equals("FRJ2")){
                        kep.FRJ2(query,k,alpha,r);
                    }
                    else if(method.equals("FRJ3")){
                        kep.FRJ3(query,k,alpha,r);
                    }
                    else if(method.equals("FRJStar")){
                        kep.FRJStar(query,k,alpha,r);
                    }
                    else {
                        System.out.println("method error.");
                        return;
                    }
                }
                Long end = System.currentTimeMillis();

                cost += (end - start);
                eventNumInvolved += kep.eventNumInvolved;
                userNumInvolved += kep.userNumInvolved;
                eventNumPruning += kep.eventNumPruning;
                eventNumPruningByOpt1 += kep.eventNumPruningByOpt1;
                round += kep.round;
                count++;
            }

            round /= max_count;
            cost /= max_count;
            eventNumInvolved /= max_count;
            userNumInvolved /= max_count;
            eventNumPruning /= max_count;
            eventNumPruningByOpt1 /= max_count;

            c.add(cost/round);
            ep.add(eventNumPruning/round);
            ec.add(eventNumInvolved/round);
            uc.add(userNumInvolved/round);
            ep2.add(eventNumPruningByOpt1/round);

            System.out.println("Method:"+method);
            System.out.println("Parameter:"+parameter);
            System.out.println("k: "+ k);
            System.out.println("keywordNum: "+ keywordNum);
            System.out.println("alpha: "+ alpha);
            System.out.println("ripple join: "+ r);
            System.out.println("round: "+ round);
            System.out.println("cost:" + cost/round);
            System.out.println("eventNumPruning:" + eventNumPruning/round);
            System.out.println("eventNumInvolved:" + eventNumInvolved/round);
            System.out.println("userNumInvolved:" + userNumInvolved/round);
            System.out.println("eventNumPruningByOpt1:" + eventNumPruningByOpt1/round);
            System.out.println();
        }

        if(parameter.equals("k") || parameter.equals("default")){
            int[] header = kTuning;
            System.out.println("***********Summary***********");
            System.out.println("Method:"+method);
            System.out.println("Parameter:"+parameter);
            for(int i=0;i<header.length;i++){
                StringBuilder sb = new StringBuilder();
                sb.append(header[i]).append(',').append(c.get(i)).append(',').append(ep.get(i)).append(',').append(ec.get(i)).append(',').append(uc.get(i)).append(',').append(ep2.get(i));
                System.out.println(sb.toString());
            }
            System.out.println("***********END***********\n");
        }
        else if(parameter.equals("keyword")){
            int[] header = keywordTuning;
            System.out.println("***********Summary***********");
            System.out.println("Method:"+method);
            System.out.println("Parameter:"+parameter);
            for(int i=0;i<header.length;i++){
                StringBuilder sb = new StringBuilder();
                sb.append(header[i]).append(',').append(c.get(i)).append(',').append(ep.get(i)).append(',').append(ec.get(i)).append(',').append(uc.get(i)).append(',').append(ep2.get(i));
                System.out.println(sb.toString());
            }
            System.out.println("***********END***********\n");
        }
        else if(parameter.equals("alpha")){
            double[] header = alphaTuning;
            System.out.println("***********Summary***********");
            System.out.println("Method:"+method);
            System.out.println("Parameter:"+parameter);
            for(int i=0;i<header.length;i++){
                StringBuilder sb = new StringBuilder();
                sb.append(header[i]).append(',').append(c.get(i)).append(',').append(ep.get(i)).append(',').append(ec.get(i)).append(',').append(uc.get(i)).append(',').append(ep2.get(i));
                System.out.println(sb.toString());
            }
            System.out.println("***********END***********\n");
        }
        else if(parameter.equals("r")){
            int[] header = rTuning;
            System.out.println("***********Summary***********");
            System.out.println("Method:"+method);
            System.out.println("Parameter:"+parameter);
            for(int i=0;i<header.length;i++){
                StringBuilder sb = new StringBuilder();
                sb.append(header[i]).append(',').append(c.get(i)).append(',').append(ep.get(i)).append(',').append(ec.get(i)).append(',').append(uc.get(i)).append(',').append(ep2.get(i));
                System.out.println(sb.toString());
            }
            System.out.println("***********END***********\n");
        }
        else {
            System.out.println("parameter error.");
            return;
        }
    }

    static void caseStudy(String method) throws Exception{

        int[] kTuning = {5};
        double[] alphaTuning = {0.2};

        for(int i=0;i<kTuning.length;i++) {

            int k = kTuning[i];
            double alpha = alphaTuning[i];

            String query;
            KEP kep = new KEP("act_processed.txt", "latlon.txt", "mem.txt");
            BufferedReader bf = new BufferedReader(new FileReader("query_testing.txt"));

            while ((query = bf.readLine()) != null) {
                if(method.equals("FNLJStar")) kep.FNLJStar4case(query,k,alpha);
                else if(method.equals("OriNLJ")) kep.OriNLJ4case(query,k,alpha);
            }

            System.out.println("mem4SEUGEML:"+kep.mem4SEUGEML*8/(1024*1024)+" MB");
            System.out.println("cost4Usort:"+kep.cost4Usort+" ms");
            System.out.println("mem4U:"+kep.mem4U+" users");
        }
    }

    static void generateQuerySet4CaseStudy(String keyword,String tails) throws Exception{

        HashSet<Integer> resultQueryUsers = new HashSet<>();
        HashMap<Integer,ArrayList<Integer>> mems = new HashMap<>();

        String line;
        BufferedReader bf = new BufferedReader(new FileReader("mem.txt"));
        while ((line = bf.readLine())!=null) {
            String[] parts = line.split(",");
            int id = Integer.parseInt(parts[0]);
            if(!mems.containsKey(id)){
                ArrayList<Integer> mem = new ArrayList<>();
                for(int i = 1;i<parts.length;i++){
                    mem.add(Integer.parseInt(parts[i]));
                }
                mems.put(id,mem);
            }
        }
        bf.close();

        bf = new BufferedReader(new FileReader("act_processed.txt"));
        while ((line = bf.readLine())!=null) {
            String[] parts = line.split(" ");
            int id = Integer.parseInt(parts[0]);
            for(int i = 1;i<parts.length;i++){
                if(parts[i].equals(keyword)){
                    resultQueryUsers.addAll(mems.get(id));
                    break;
                }
            }
        }
        bf.close();

        PrintStream ps = new PrintStream(new FileOutputStream("query_testing.txt"));
        Iterator<Integer> iterator = resultQueryUsers.iterator();
        while (iterator.hasNext()){
            ps.println(iterator.next()+"|"+keyword+" "+tails+"|"+0+"|"+0);
        }
    }

    static void selectCase() throws Exception{

        String line;
        HashMap<Integer,ArrayList<String>> OriNLJ = new HashMap<>();
        BufferedReader bf = new BufferedReader(new FileReader("OriNLJ4case.txt"));
        while ((line = bf.readLine())!=null) {
            if(line.contains("userID: ")){
                int userID = Integer.parseInt(line.split(" ")[1]);
                OriNLJ.put(userID,new ArrayList<>());
                String tmp;
                while ((tmp = bf.readLine())!=null){
                    if(tmp.equals("")) break;
                    else if(tmp.contains("queryKeywords:")) continue;
                    else OriNLJ.get(userID).add(tmp);
                }
            }
        }
        bf.close();

        HashMap<Integer,ArrayList<String>> FNLJStar = new HashMap<>();
        bf = new BufferedReader(new FileReader("FNLJStar4case.txt"));
        while ((line = bf.readLine())!=null) {
            if(line.contains("userID: ")){
                int userID = Integer.parseInt(line.split(" ")[1]);
                FNLJStar.put(userID,new ArrayList<>());
                String tmp;
                while ((tmp = bf.readLine())!=null){
                    if(tmp.equals("")) break;
                    else if(tmp.contains("queryKeywords:")) continue;
                    else FNLJStar.get(userID).add(tmp);
                }
            }
        }
        bf.close();

        PrintStream ps = new PrintStream(new FileOutputStream("case.txt"));
        for(HashMap.Entry<Integer,ArrayList<String>> entry : OriNLJ.entrySet()){
            if(entry.getValue().size()>0 && FNLJStar.containsKey(entry.getKey()) && FNLJStar.get(entry.getKey()).size()>3){
                ps.println("userID:"+entry.getKey());
                ps.println("OriNLJ:");
                for(int i=0;i<entry.getValue().size();i++){
                    ps.println(entry.getValue().get(i));
                }
                ps.println("FNLJStar:");
                for(int i=0;i<FNLJStar.get(entry.getKey()).size();i++){
                    ps.println(FNLJStar.get(entry.getKey()).get(i));
                }
                ps.println();
            }
        }
    }
}
