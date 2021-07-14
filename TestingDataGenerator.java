import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.*;

class TestingDataGenerator {
    
    void generateQuerySet1(String fileEvent, String fileUser, String fileQuery) throws Exception{

        ArrayList<Integer> eventSet = new ArrayList<>();
        ArrayList<Integer> userSet = new ArrayList<>();

        BufferedReader bf = new BufferedReader(new FileReader(fileEvent));
        String line;
        while ((line = bf.readLine())!=null) {
            String[] parts = line.split(" ");
            int eventID = Integer.parseInt(parts[0]);
            eventSet.add(eventID);
        }
        bf.close();

        bf = new BufferedReader(new FileReader(fileUser));
        while ((line = bf.readLine())!=null) {
            String userID;
            String[] parts = line.split(",");
            for(int i=1;i<parts.length;i++){
                userID = parts[i];
                userSet.add(Integer.parseInt(userID));
            }
        }
        bf.close();

        HashMap<Integer,ArrayList<String>> eventsDocument = new HashMap<>();
        bf = new BufferedReader(new FileReader("act_processed.txt"));
        while ((line = bf.readLine())!=null) {
            String[] parts = line.split(" ");
            eventsDocument.put(Integer.parseInt(parts[0]),new ArrayList<>());
            ArrayList<String> doucument = eventsDocument.get(Integer.parseInt(parts[0]));
            for(int i=1;i<parts.length;i++){
                doucument.add(parts[i]);
            }
        }
        bf.close();

        int keywordNumber = 1;
        int queryNumber = 100;
        while (keywordNumber<=5){
            PrintStream ps = new PrintStream(new FileOutputStream(keywordNumber+fileQuery));
            int count = 0;
            while (count < queryNumber) {
                Random random = new Random(count + keywordNumber*1000);
                int eventIndex = random.nextInt(eventSet.size());
                int userIndex = random.nextInt(userSet.size());
                int eventID = eventSet.get(eventIndex);
                int userID = userSet.get(userIndex);
                double latitude = random.nextDouble();
                double longitude = random.nextDouble();
                String keywords = "";
                ArrayList<String> document = eventsDocument.get(eventSet.get(eventIndex));
                for(int i=0;i<keywordNumber;i++){
                    keywords+=(document.get(random.nextInt(document.size()))+" ");
                }
                ps.println(userID+"|"+keywords+"|"+longitude+"|"+latitude);
                count++;
            }
            keywordNumber++;
            bf.close();
        }
    }

    void generateTestingDataset(String fileEvent, String fileUser, String output1, String output2,int seed) throws Exception {

        ArrayList<Integer> eventSet = new ArrayList<>();
        HashMap<Integer, HashSet<Integer>> participants2Events = new HashMap<>();

        BufferedReader bf = new BufferedReader(new FileReader(fileEvent));
        String line;
        while ((line = bf.readLine()) != null) {
            String[] parts = line.split(" ");
            int eventID = Integer.parseInt(parts[0]);
            eventSet.add(eventID);
        }
        bf.close();

        bf = new BufferedReader(new FileReader(fileUser));
        while ((line = bf.readLine()) != null) {
            int eventID;
            int participantID;
            String[] parts = line.split(",");
            eventID = Integer.parseInt(parts[0]);
            for (int i = 1; i < parts.length; i++) {
                participantID = Integer.parseInt(parts[i]);
                if (!participants2Events.containsKey(eventID)) {
                    participants2Events.put(eventID, new HashSet<>());
                    participants2Events.get(eventID).add(participantID);
                } else {
                    participants2Events.get(eventID).add(participantID);
                }
            }
        }
        bf.close();

        double percentage = 0.1;
        Random random = new Random(seed);
        int testingScale = (int)(eventSet.size()*percentage);
        int count = 0;
        HashSet<Integer> testingEvents = new HashSet<>();
        while (count<=testingScale){
            int eventID = random.nextInt(eventSet.size());
            testingEvents.add(eventSet.get(eventID));
            eventSet.remove(eventID);
            count++;
        }

        PrintStream ps1 = new PrintStream(new FileOutputStream(output1));
        PrintStream ps2 = new PrintStream(new FileOutputStream(output2));
        for(HashMap.Entry<Integer,HashSet<Integer>> entry : participants2Events.entrySet()){
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey()).append(',');
            Iterator<Integer> iterator = entry.getValue().iterator();
            while(iterator.hasNext()){
                sb.append(iterator.next()).append(',');
            }
            if(testingEvents.contains(entry.getKey())){
                ps2.println(sb.toString());
            }
            else {
                ps1.println(sb.toString());
            }
        }
    }

    void generateQueryset2(String mem2Event, String info2Event, String loc2Event, String testingQueryset,int seed,int keywordNumber) throws Exception{

        int queryNum = 200;

        ArrayList<Integer> events = new ArrayList<>();

        HashMap<Integer,ArrayList<String>> infos = new HashMap<>();
        HashMap<Integer,String> locs = new HashMap<>();
        HashMap<Integer,ArrayList<Integer>> mems = new HashMap<>();

        String line;
        BufferedReader bf = new BufferedReader(new FileReader(info2Event));
        while ((line = bf.readLine())!=null) {
            String[] parts = line.split(" ");
            int id = Integer.parseInt(parts[0]);
            if(!infos.containsKey(id)){
                ArrayList<String> info = new ArrayList<>();
                for(int i = 1;i<parts.length;i++){
                    info.add(parts[i]);
                }
                infos.put(id,info);
            }
        }
        bf.close();

        bf = new BufferedReader(new FileReader(loc2Event));
        while ((line = bf.readLine())!=null) {
            String[] parts = line.split(",");
            int id = Integer.parseInt(parts[0]);
            if(!locs.containsKey(id)){
                StringBuilder loc = new StringBuilder();
                loc.append(parts[3]).append(' ').append(parts[2]);
                locs.put(id,loc.toString());
            }
        }
        bf.close();

        bf = new BufferedReader(new FileReader(mem2Event));
        while ((line = bf.readLine())!=null) {
            String[] parts = line.split(",");
            int id = Integer.parseInt(parts[0]);
            if(!mems.containsKey(id)){
                ArrayList<Integer> mem = new ArrayList<>();
                for(int i = 1;i<parts.length;i++){
                    mem.add(Integer.parseInt(parts[i]));
                }
                mems.put(id,mem);
                events.add(id);
            }
        }
        bf.close();

        int count = 0;
        Random random = new Random(seed);
        PrintStream ps = new PrintStream(new FileOutputStream(testingQueryset));
        while (count<queryNum){

            int index = random.nextInt(events.size());
            int eventID = events.get(index);
            events.remove(index);
            ArrayList<String> info = infos.get(eventID);
            String keywords = "";
            for(int i=0;i<keywordNumber;i++){
                if(info.size()==0){
                    break;
                }
                index = random.nextInt(info.size());
                keywords+=(info.get(index)+" ");
                info.remove(index);
            }

            ArrayList<Integer> participants = mems.get(eventID);
            index = random.nextInt(participants.size());
            int userID = participants.get(index);

            String loc = locs.get(eventID);
            double longitude = Double.parseDouble((loc.split(" "))[0]);
            double latitude = Double.parseDouble((loc.split(" "))[1]);

            ps.println(userID+"|"+keywords+"|"+longitude+"|"+latitude);
            count++;
        }
    }

}
