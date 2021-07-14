import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class RecommendingEPCompartor {
    double compare(String realData,String recommendingData) throws Exception{
        
        HashMap<Integer, HashSet<Integer>> realPartners = new HashMap<>();
        BufferedReader bf = new BufferedReader(new FileReader(realData));
        String line;
        while ((line = bf.readLine()) != null) {
            String[] parts = line.split(",");
            int eventID = Integer.parseInt(parts[0]);
            HashSet<Integer> partners = new HashSet<>();
            for (int i=1;i<parts.length;i++){
                partners.add(Integer.parseInt(parts[i]));
            }
            realPartners.put(eventID,partners);
        }
        bf.close();

        int queryNum = 0;
        bf = new BufferedReader(new FileReader(recommendingData));
        while ((line = bf.readLine()) != null) {
            queryNum++;
        }

        double TP = 0;
        bf = new BufferedReader(new FileReader(recommendingData));
        while ((line = bf.readLine()) != null) {
            String[] parts = line.split(" ");
            for (int i=0;i<parts.length;i++){
                String tuple = parts[i];
                if(tuple.contains(",")){
                    int eventID = Integer.parseInt((tuple.split(","))[0]);
                    int partner = Integer.parseInt((tuple.split(","))[1]);
                    if(realPartners.get(eventID).contains(partner)){
                        TP++;
                        break;
                    }
                }
            }
        }
        bf.close();
        double precision = TP/queryNum;

        File file = new File(recommendingData);
        FileWriter fileWriter =new FileWriter(file);
        fileWriter.write("");
        fileWriter.flush();
        fileWriter.close();

        return precision;
    }
}
