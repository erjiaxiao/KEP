import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.*;

class KEP {

    class EventObject{
        int eventID;
        HashMap<String,Integer> wordsCount;
        double eventLongitude;
        double eventLatitude;
    }

    class QueryParameter{
        int queryUserID;
        HashMap<String,Integer> queryWordsCount;
        double queryLongitude;
        double queryLatitude;

        EventObject queryObject;

        int queryTopK = 5;

        double textualWeight = 0.2;
        double togetherWeight = 0.8;

        double neighborTextualParam = 0.1;

        int num_keyword = 3;
    }

    HashMap<Integer,EventObject> eventsInfo;
    HashMap<Integer,HashSet<Integer>> eventToUsers;
    HashMap<Integer,HashSet<Integer>> userToEvent;

    double threshold;

    ArrayList<Integer> attendedEvents;
    PriorityQueue<Pair> nextEventQueue;
    PriorityQueue<Pair> topkEventPartnerQueue;
    QueryParameter param;

    long calPre = 0;
    long calN = 0;
    long calU = 0;
    long calPartner = 0;
    long cal1 = 0;
    long cal2 = 0;
    long cal3 = 0;

    int TAtimes = 0;
    int round = 0;

    long eventNumInvolved = 0;
    long userNumInvolved = 0;
    long eventNumPruning = 0;
    long eventNumPruningByOpt1 = 0;

    int step = 1;
    int augment = 1;
    HashMap<Integer,HashMap<Integer,Double>> mapEventsSimilarity;
    HashMap<Integer,Double> mapNDenominator;
    HashMap<Integer,ArrayList<Double>> mapNSimilaritySumList;
    HashMap<Integer,HashSet<Integer>> mapRelatedEvents;
    HashMap<Integer,Pair> mapTempEP;
    HashMap<Integer,HashMap<Integer,Integer>> mapRCount;
    HashMap<Integer,HashMap<Integer,Integer>> mapECount;
    HashMap<Integer,REIndex> mapREIndex;
    HashMap<Integer,HashSet<Integer>> mapPresentU;
    HashMap<Integer,Integer> mapScannedInPresentU;

    HashMap<Integer,PriorityQueue<Pair>> mapEventNum4partner;

    HashMap<String,ArrayList<Integer>> InvertedIndex = new HashMap<>();

    double bm25k = 1.25;
    double bm25b = 0.75;
    double factor = 1;

    double avgQueryTextualSimilarity = 0;
    double avgNorQueryTextualSimilarity = 0;
    int topkEventnumber = 0;

    HashMap<Integer,Integer> partitionIndex = new HashMap<>();
    HashMap<Integer,HashSet<Integer>> partition = new HashMap<>();
    int match = 0;
    int total = 0;

    KEP(String fileEvent,String fileLocality,String fileEventUser) throws Exception{
        this.eventsInfo = new HashMap<>();
        this.eventToUsers = new HashMap<>();
        this.userToEvent = new HashMap<>();

        BufferedReader bf = new BufferedReader(new FileReader(fileEvent));
        String line;
        while ((line = bf.readLine())!=null) {
            String[] parts = line.split(" ");
            int eventID = Integer.parseInt(parts[0]);
            EventObject eventObject = new EventObject();
            eventObject.eventID = eventID;
            this.eventsInfo.put(eventID,eventObject);
        }
        bf.close();

        bf = new BufferedReader(new FileReader(fileLocality));
        while ((line = bf.readLine())!=null) {
            String[] parts = line.split(",");
            int eventID = Integer.parseInt(parts[0]);
            if(!eventsInfo.containsKey(eventID)){
                continue;
            }
            EventObject eventObject = eventsInfo.get(eventID);
            eventObject.eventLatitude = Double.parseDouble(parts[parts.length-2]);
            eventObject.eventLongitude = Double.parseDouble(parts[parts.length-1]);
        }
        bf.close();

        bf = new BufferedReader(new FileReader(fileEvent));
        while ((line = bf.readLine())!=null) {
            String[] parts = line.split(" ");
            EventObject eventObject = this.eventsInfo.get(Integer.parseInt(parts[0]));
            eventObject.wordsCount = new HashMap<>();
            for(int i=1;i<parts.length;i++){
                if (!eventObject.wordsCount.containsKey(parts[i])) {
                    eventObject.wordsCount.put(parts[i],1);
                }
                else{
                    int currCount=eventObject.wordsCount.get(parts[i])+1;
                    eventObject.wordsCount.remove(parts[i]);
                    eventObject.wordsCount.put(parts[i],currCount);
                }
            }
        }
        bf.close();

        bf = new BufferedReader(new FileReader(fileEventUser));
        while ((line = bf.readLine())!=null) {
            int EID,UID;
            String[] parts = line.split(",");
            EID = Integer.parseInt(parts[0]);
            for(int i=1;i<parts.length;i++){
                UID = Integer.parseInt(parts[i]);
                if(!this.userToEvent.containsKey(UID)) {
                    this.userToEvent.put(UID,new HashSet<>());
                }
                this.userToEvent.get(UID).add(EID);
            }
        }
        bf.close();

        bf = new BufferedReader(new FileReader(fileEventUser));
        while ((line = bf.readLine())!=null) {
            int EID,UID;
            String[] parts = line.split(",");
            EID = Integer.parseInt(parts[0]);
            if(!this.eventToUsers.containsKey(EID)) {
                this.eventToUsers.put(EID,new HashSet<>());
            }
            for(int i=1;i<parts.length;i++){
                UID = Integer.parseInt(parts[i]);
                this.eventToUsers.get(EID).add(UID);
            }
        }
        bf.close();

        initializeInvertedIndex();
        //initializeGraphPartition();
    }

    void initializeInvertedIndex() throws Exception{
        String line;
        BufferedReader bf = new BufferedReader(new FileReader("act_processed.txt"));

        while ((line = bf.readLine()) != null) {
            String[] parts = line.split(" ");
            int eventID = Integer.parseInt(parts[0]);
            for(int i=1;i<parts.length;i++){
                String keyword = parts[i];
                if (!InvertedIndex.containsKey(keyword)){
                    InvertedIndex.put(keyword,new ArrayList<>());
                }
                InvertedIndex.get(keyword).add(eventID);
            }
        }
    }

    void initializeGraphPartition() throws Exception{
        String line;
        BufferedReader bf = new BufferedReader(new FileReader("GraphPartition"));
        while ((line = bf.readLine()) != null) {
            String[] parts = line.split(" ");
            int node = Integer.parseInt(parts[0]);
            int cluster = Integer.parseInt(parts[1]);
            partitionIndex.put(node,cluster);
            if(!partition.containsKey(cluster)){
                partition.put(cluster,new HashSet<>());
            }
            partition.get(cluster).add(node);
        }
    }

    void reset(){
        this.param = new QueryParameter();
        this.attendedEvents = new ArrayList<>();
        this.threshold = Double.MAX_VALUE;

        Comparator<Pair> customComparator = new PairScoreComparator();
        this.nextEventQueue = new PriorityQueue<>(customComparator);
        this.topkEventPartnerQueue = new PriorityQueue<>(customComparator);

        this.step = 1;
        this.mapEventsSimilarity = new HashMap<>();
        this.mapNDenominator = new HashMap<>();
        this.mapNSimilaritySumList = new HashMap<>();
        this.mapRelatedEvents = new HashMap<>();
        this.mapTempEP = new HashMap<>();
        this.mapECount = new HashMap<>();
        this.mapRCount = new HashMap<>();
        this.mapREIndex = new HashMap<>();
        this.mapPresentU = new HashMap<>();
        this.mapScannedInPresentU = new HashMap<>();

        this.mapEventNum4partner = new HashMap<>();

        this.factor = 1;
    }

    void setQuery(String query) {

        String[] parts = query.split("\\|");                  
        this.param = new QueryParameter();
        this.param.queryUserID = Integer.parseInt(parts[0]);
        this.param.queryLongitude = Double.parseDouble(parts[2]);
        this.param.queryLatitude = Double.parseDouble(parts[3]);

        this.param.queryWordsCount = new HashMap<>();
        String[] words = (parts[1]).split(" ");                              
        for(int i=0;i<words.length;i++){
            if (!this.param.queryWordsCount.containsKey(words[i])) {
                this.param.queryWordsCount.put(words[i],1);
            }
            else{
                int currCount=this.param.queryWordsCount.get(words[i])+1;
                this.param.queryWordsCount.remove(words[i]);
                this.param.queryWordsCount.put(words[i],currCount);
            }
        }

        this.param.queryObject = new EventObject();
        this.param.queryObject.eventLatitude = this.param.queryLatitude;
        this.param.queryObject.eventLongitude = this.param.queryLongitude;
        this.param.queryObject.wordsCount = this.param.queryWordsCount;
        this.param.queryObject.eventID = this.param.queryUserID;              
    }

    void getAttendedEvent(){
        for(HashMap.Entry<Integer, HashSet<Integer>> entry1: this.eventToUsers.entrySet())
        {
            HashSet<Integer> hs = entry1.getValue();
            if(hs.contains(this.param.queryUserID)) {
                this.attendedEvents.add(entry1.getKey());
            }
        }
    }

    Pair getNextEvent(){
        Pair nextEvent = nextEventQueue.poll();
        return nextEvent;
    }

    double calculateTextualSimilarity(EventObject o1, EventObject o2){
        double similarityValue = 0;
        double denominator = 0;
        double numerator = 0;
        double v1=0,v2=0,tmp;
        for(HashMap.Entry<String, Integer> entry1: o1.wordsCount.entrySet())
        {
            tmp = entry1.getValue();
            v1+=(tmp*tmp);

            String word = entry1.getKey();
            if(o2.wordsCount.containsKey(word)){
                numerator+=(o1.wordsCount.get(word)*o2.wordsCount.get(word));
            }
        }
        for(HashMap.Entry<String, Integer> entry2: o2.wordsCount.entrySet())
        {
            tmp = entry2.getValue();
            v2+=(tmp*tmp);
        }
        denominator = Math.sqrt(v1)*Math.sqrt(v2);

        similarityValue = numerator/denominator;
        return similarityValue;
    }

    double calculateQueryTextualSimilarity(EventObject query, EventObject doc){

        double result = 0;

        double totalDocNum = eventsInfo.size();
        double avgDocLen = 10;

        for(HashMap.Entry<String, Integer> entry: query.wordsCount.entrySet())
        {
            String word = entry.getKey();
            int frequency = 0;
            if(doc.wordsCount.containsKey(word)){
                frequency = doc.wordsCount.get(word);
            }

            int docNum = 0;
            if(InvertedIndex.containsKey(word)){
                docNum = InvertedIndex.get(word).size();
            }
            double idf = ((totalDocNum-docNum+0.5)/(docNum+0.5));
            idf = Math.log(idf)/Math.log(2);

            double score = (frequency*(bm25k+1))/(frequency+bm25k*(1-bm25b+bm25b*(doc.wordsCount.size()/avgDocLen)));

            result+=(idf*score);
        }

        return (result/factor);
    }

    boolean isNeighborhood(EventObject o1, EventObject o2){
        if(calculateTextualSimilarity(o1,o2)>=this.param.neighborTextualParam){
            return true;
        }
        else{
            return false;
        }
    }

    double calculateTopKthValue(){

        int count = 0;
        ArrayList<Pair> tmp = new ArrayList<>();
        while (count<param.queryTopK){
            tmp.add(topkEventPartnerQueue.poll());
            count++;
        }

        for(int i = 0;i<tmp.size();i++){
            Pair curr = tmp.get(i);
            topkEventPartnerQueue.add(curr);
        }

        double value = tmp.get(tmp.size()-1).score;
        return value;
    }

    void printTopkPairs(String filename) throws Exception{
        File file = new File(filename);
        if(!file.exists()){
            file.createNewFile();
        }

        StringBuilder str = new StringBuilder();
        for(int i=0;i<param.queryTopK;i++){
            Pair pair = topkEventPartnerQueue.poll();
            if(pair == null) {
                break;
            }
            str.append(pair.eventID).append(",").append(pair.partnerID).append(",").append(pair.score).append("      ");
            avgQueryTextualSimilarity+=(calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(pair.eventID))*factor);
            avgNorQueryTextualSimilarity+=(calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(pair.eventID)));
            topkEventnumber++;
        }
        str.append("\n");

        String line = str.toString();
        FileOutputStream out = null;
        byte[] buff = new byte[]{};
        out = new FileOutputStream(file,true);
        out.write(line.getBytes());
        out.flush();
        out.close();
    }

    void FSRJFNLJAccuracyWithII(int n) throws Exception{

        System.out.println("***********CASE Start: top-k = "+ n +"***********");

        reset();

        String line;
        HashSet<Integer> testingEvents = new HashSet<>();
        HashSet<Integer> testingUsers = new HashSet<>();
        HashMap<Integer,HashSet<Integer>> testingUsers2testingEvents = new HashMap<>();

        BufferedReader bf = new BufferedReader(new FileReader("mem_testing.txt"));
        while ((line = bf.readLine()) != null) {
            String[] parts = line.split(",");
            int eventID = Integer.parseInt(parts[0]);
            if(!testingEvents.contains(eventID)){
                testingEvents.add(eventID);
                testingUsers2testingEvents.put(eventID,new HashSet<>());
                for(int i=1;i<parts.length;i++){
                    int userID = Integer.parseInt(parts[i]);
                    if(!testingUsers.contains(userID)){
                        testingUsers.add(userID);
                    }
                    testingUsers2testingEvents.get(eventID).add(userID);
                }
            }
        }
        bf.close();

        HashMap<Integer,HashSet<Integer>> neighborhood_events = new HashMap<>();
        HashMap<Integer,HashMap<Integer,Double>> similarity_events = new HashMap<>();

        int num_hit_FSLJ = 0;
        int num_hit_FNLJ = 0;
        int num_testcase = 0;

        Iterator<Integer> iterator = testingEvents.iterator();

        while(iterator.hasNext()){
            int targetEvent = iterator.next();

            if(testingUsers2testingEvents.get(targetEvent).size()<2){
                continue;
            }
            Iterator<Integer> it = testingUsers2testingEvents.get(targetEvent).iterator();
            int user = it.next();
            int partner = it.next();

            double value_FSLJ = calculateValueFSRJWithII(user,targetEvent,partner,neighborhood_events,similarity_events);
            double value_FNLJ = calculateValueFNLJWithII(user,targetEvent,partner,neighborhood_events,similarity_events);

            double value_keyword = calculateValueKeyword(targetEvent,targetEvent);
            value_FSLJ = this.param.textualWeight*value_keyword+this.param.togetherWeight*value_FSLJ;
            value_FNLJ = this.param.textualWeight*value_keyword+this.param.togetherWeight*value_FNLJ;

            if(value_FNLJ==0 && value_FSLJ==0){
                continue;
            }

            int rank_FSLJ = 0;
            int rank_FNLJ = 0;
            int count = 0;
            it = testingUsers.iterator();
            while (it.hasNext()){
                if(count>=500){
                    break;
                }
                int curr_partner = it.next();
                if(testingUsers2testingEvents.get(targetEvent).contains(user) && testingUsers2testingEvents.get(targetEvent).contains(curr_partner)){
                    continue;
                }

                double curr_value = calculateValueFSRJWithII(user,targetEvent,curr_partner,neighborhood_events,similarity_events);
                curr_value = this.param.textualWeight*value_keyword+this.param.togetherWeight*curr_value;

                if(curr_value>=value_FSLJ){
                    rank_FSLJ++;
                }

                curr_value = calculateValueFNLJWithII(user,targetEvent,curr_partner,neighborhood_events,similarity_events);
                curr_value = this.param.textualWeight*value_keyword+this.param.togetherWeight*curr_value;

                if(curr_value>=value_FNLJ){
                    rank_FNLJ++;
                }

                count++;
            }

            count = 0;
            it = testingEvents.iterator();
            while (it.hasNext()){
                if(count>=500){
                    break;
                }
                int curr_event = it.next();
                if(testingUsers2testingEvents.get(curr_event).contains(user) && testingUsers2testingEvents.get(curr_event).contains(partner)){
                    continue;
                }

                double curr_value = calculateValueFSRJWithII(user,curr_event,partner,neighborhood_events,similarity_events);
                double curr_value_keyword = calculateValueKeyword(targetEvent,curr_event);
                curr_value = this.param.textualWeight*curr_value_keyword+this.param.togetherWeight*curr_value;

                if(curr_value>=value_FSLJ){
                    rank_FSLJ++;
                }

                curr_value = calculateValueFNLJWithII(user,curr_event,partner,neighborhood_events,similarity_events);
                curr_value = this.param.textualWeight*curr_value_keyword+this.param.togetherWeight*curr_value;

                if(curr_value>=value_FNLJ){
                    rank_FNLJ++;
                }

                count++;
            }

            if(rank_FSLJ<=n){
                num_hit_FSLJ++;
            }

            if(rank_FNLJ<=n){
                num_hit_FNLJ++;
            }

            System.out.println("FSRJhits:"+ num_hit_FSLJ+"  FSRJrank:"+rank_FSLJ);
            System.out.println("FNLJhits:"+ num_hit_FNLJ+"  FNLJrank:"+rank_FNLJ);

            num_testcase++;
            System.out.println("num_testcase:"+num_testcase+"\n");

            if(num_testcase==100) {
                break;
            }
        }

        System.out.println("***********CASE END***********\n");
    }

    double calculateValueFSRJWithII(int user,int event,int partner,HashMap<Integer,HashSet<Integer>> neighborhood_events,HashMap<Integer,HashMap<Integer,Double>> similarity_events) {

        EventObject currEvent = eventsInfo.get(event);

        HashSet<Integer> events_user = userToEvent.get(user);
        HashSet<Integer> events_partner = userToEvent.get(partner);
        HashSet<Integer> relatedEvents = new HashSet<>();

        if(events_user==null) {
            events_user = new HashSet<>();
        }
        if(events_partner==null){
            events_partner = new HashSet<>();
        }

        if(neighborhood_events.containsKey(event)){
            relatedEvents = neighborhood_events.get(event);
        }
        else {
            HashSet<Integer> candidateEvents = getRelatedEventFromInvertedIndex(currEvent);
            Iterator<Integer> iterator = candidateEvents.iterator();
            while(iterator.hasNext()){
                int eventID = iterator.next();
                if (isNeighborhood(currEvent, eventsInfo.get(eventID))) {
                    relatedEvents.add(eventID);
                }
            }
            neighborhood_events.put(event,relatedEvents);
        }

        double nominator = 0;
        double denominator = 0;
        Iterator<Integer> iterator = relatedEvents.iterator();

        HashMap<Integer, Double> similarity_targetEvent;
        HashMap<Integer, Double> similarity_currEvent;
        if(!similarity_events.containsKey(event)) {
            similarity_events.put(event,new HashMap<>());
        }
        similarity_targetEvent = similarity_events.get(event);

        while (iterator.hasNext()) {
            int curr_event = iterator.next();

            double similarity;
            if(!similarity_events.containsKey(curr_event)) {
                similarity_events.put(curr_event,new HashMap<>());
            }
            similarity_currEvent = similarity_events.get(curr_event);

            if(similarity_targetEvent.containsKey(curr_event)){
                similarity = similarity_targetEvent.get(curr_event);
            }
            else {
                double textualValue = calculateTextualSimilarity(currEvent, eventsInfo.get(curr_event));
                similarity = textualValue;
                similarity_targetEvent.put(curr_event,similarity);
                similarity_currEvent.put(event,similarity);
            }

            if (events_partner.contains(curr_event)) {
                nominator += similarity;
            }
            denominator += similarity;
        }

        iterator = events_partner.iterator();
        int size_events_partner = 0;
        while (iterator.hasNext()){
            if(events_user.contains(iterator.next())){
                size_events_partner++;
            }
        }
        double proximity = size_events_partner / (double) events_user.size();

        if(denominator==0 || events_user.size()==0) {
            return 0;
        }
        else {
            double togetherValue = (nominator / denominator) * proximity;
            return togetherValue;
        }
    }

    double calculateValueFNLJWithII(int user,int event,int partner,HashMap<Integer,HashSet<Integer>> neighborhood_events,HashMap<Integer,HashMap<Integer,Double>> similarity_events) {

        EventObject currEvent = eventsInfo.get(event);

        HashSet<Integer> events_user = userToEvent.get(user);
        HashSet<Integer> events_partner = userToEvent.get(partner);
        HashSet<Integer> relatedEvents = new HashSet<>();

        if(events_user==null) {
            events_user = new HashSet<>();
        }
        if(events_partner==null){
            events_partner = new HashSet<>();
        }

        if(neighborhood_events.containsKey(event)){
            relatedEvents = neighborhood_events.get(event);
        }
        else {
            HashSet<Integer> candidateEvents = getRelatedEventFromInvertedIndex(currEvent);
            Iterator<Integer> iterator = candidateEvents.iterator();
            while(iterator.hasNext()){
                int eventID = iterator.next();
                if (isNeighborhood(currEvent, eventsInfo.get(eventID))) {
                    relatedEvents.add(eventID);
                }
            }
            neighborhood_events.put(event,relatedEvents);
        }

        double nominator = 0;
        double denominator = 0;
        Iterator<Integer> iterator = events_user.iterator();

        HashMap<Integer, Double> similarity_targetEvent;
        HashMap<Integer, Double> similarity_currEvent;
        if(!similarity_events.containsKey(event)) {
            similarity_events.put(event,new HashMap<>());
        }
        similarity_targetEvent = similarity_events.get(event);

        while (iterator.hasNext()) {
            int curr_event = iterator.next();

            double similarity;
            if(!similarity_events.containsKey(curr_event)) {
                similarity_events.put(curr_event,new HashMap<>());
            }
            similarity_currEvent = similarity_events.get(curr_event);

            if(similarity_targetEvent.containsKey(curr_event)){
                similarity = similarity_targetEvent.get(curr_event);
            }
            else {
                double textualValue = calculateTextualSimilarity(currEvent, eventsInfo.get(curr_event));
                similarity = textualValue;
                similarity_targetEvent.put(curr_event,similarity);
                similarity_currEvent.put(event,similarity);
            }

            if (relatedEvents.contains(curr_event)) {
                if(events_partner.contains(curr_event)){
                    nominator += similarity;
                }
                denominator += similarity;
            }
        }

        if(denominator==0) {
            return 0;
        }
        else {
            double togetherValue = (nominator / denominator);
            return togetherValue;
        }
    }

    HashSet<Integer> getRelatedEventFromInvertedIndex(EventObject currEvent){
        HashSet<Integer> candidateEvents=new HashSet<>();
        for (Map.Entry<String, Integer> entry : currEvent.wordsCount.entrySet()) {
            String word = entry.getKey();
            if(InvertedIndex.containsKey(word)) candidateEvents.addAll(InvertedIndex.get(word));
        }
        return candidateEvents;
    }

    void KeywordAccuracyVer1() throws Exception{

        reset();

        String line;
        HashSet<Integer> testingEvents = new HashSet<>();
        HashSet<Integer> testingUsers = new HashSet<>();
        HashMap<Integer,HashSet<Integer>> testingUsers2testingEvents = new HashMap<>();

        BufferedReader bf = new BufferedReader(new FileReader("mem_testing.txt"));
        while ((line = bf.readLine()) != null) {
            String[] parts = line.split(",");
            int eventID = Integer.parseInt(parts[0]);
            if(!testingEvents.contains(eventID)){
                testingEvents.add(eventID);
                testingUsers2testingEvents.put(eventID,new HashSet<>());
                for(int i=1;i<parts.length;i++){
                    int userID = Integer.parseInt(parts[i]);
                    if(!testingUsers.contains(userID)){
                        testingUsers.add(userID);
                    }
                    testingUsers2testingEvents.get(eventID).add(userID);
                }
            }
        }
        bf.close();

        int num_hit_keyword = 0;
        int num_testcase = 0;

        Iterator<Integer> iterator = testingEvents.iterator();

        while(iterator.hasNext()){
            int targetEvent = iterator.next();

            if(testingUsers2testingEvents.get(targetEvent).size()<2){
                continue;
            }
            Iterator<Integer> it = testingUsers2testingEvents.get(targetEvent).iterator();
            int user = it.next();
            int partner = it.next();

            double value_keyword = calculateValueKeyword(targetEvent,targetEvent);

            int rank_keyword = 0;
            int count = 0;
            it = testingEvents.iterator();
            while (it.hasNext()){
                if(count>=1000){
                    break;
                }
                int curr_event = it.next();
                if(testingUsers2testingEvents.get(curr_event).contains(user) && testingUsers2testingEvents.get(curr_event).contains(partner)){
                    continue;
                }

                double curr_value_keyword = calculateValueKeyword(targetEvent,curr_event);

                if(curr_value_keyword>=value_keyword){
                    rank_keyword++;
                }

                count++;
            }

            System.out.println("Keywordrank:"+rank_keyword);

            num_testcase++;
            System.out.println("num_testcase:"+num_testcase+"\n");

            if(num_testcase==101) {
                break;
            }
        }

        System.out.println("***********CASE END***********\n");
    }

    void KeywordAccuracyVer2() throws Exception{

        reset();

        String line;
        HashSet<Integer> testingEvents = new HashSet<>();
        HashSet<Integer> testingUsers = new HashSet<>();
        HashMap<Integer,HashSet<Integer>> testingUsers2testingEvents = new HashMap<>();

        BufferedReader bf = new BufferedReader(new FileReader("mem_testing.txt"));
        while ((line = bf.readLine()) != null) {
            String[] parts = line.split(",");
            int eventID = Integer.parseInt(parts[0]);
            if(!testingEvents.contains(eventID)){
                testingEvents.add(eventID);
                testingUsers2testingEvents.put(eventID,new HashSet<>());
                for(int i=1;i<parts.length;i++){
                    int userID = Integer.parseInt(parts[i]);
                    if(!testingUsers.contains(userID)){
                        testingUsers.add(userID);
                    }
                    testingUsers2testingEvents.get(eventID).add(userID);
                }
            }
        }
        bf.close();

        int num_testcase = 0;
        ArrayList<Integer> testcases = new ArrayList<>();
        Iterator<Integer> iterator = testingEvents.iterator();
        while(iterator.hasNext()) {

            int targetEvent = iterator.next();
            if (testingUsers2testingEvents.get(targetEvent).size() < 2) {
                continue;
            }
            testcases.add(targetEvent);

            num_testcase++;
            if(num_testcase==5) {
                break;
            }
        }

        ArrayList<String> wordset = new ArrayList<>();
        HashSet<String> unique = new HashSet<>();
        for(int i=0;i<testcases.size();i++){
            int eventID = testcases.get(i);
            EventObject currEvent = eventsInfo.get(eventID);
            for(HashMap.Entry<String, Integer> entry: currEvent.wordsCount.entrySet())
            {
                String currWord = entry.getKey();
                if(!unique.contains(currWord)){
                    wordset.add(currWord);
                    unique.add(currWord);
                }
            }
        }

        num_testcase = 0;
        iterator = testingEvents.iterator();
        while(iterator.hasNext()){

            int targetEvent = iterator.next();

            if(testingUsers2testingEvents.get(targetEvent).size()<2){
                continue;
            }

            Iterator<Integer> it = testingUsers2testingEvents.get(targetEvent).iterator();
            int user = it.next();
            int partner = it.next();

            EventObject keywordEvent = new EventObject();
            keywordEvent.wordsCount = new HashMap<>();
            Random random = new Random(num_testcase*1024);
            for(int i = 0;i<param.num_keyword;i++){
                String nextWord = wordset.get(random.nextInt(wordset.size()));
                if(keywordEvent.wordsCount.containsKey(nextWord)){
                    i--;
                }
                else {
                    keywordEvent.wordsCount.put(nextWord,1);
                }
            }

            double denominator;
            double numerator = 0;
            double v1=0,v2=0,tmp;
            for(HashMap.Entry<String, Integer> entry1: keywordEvent.wordsCount.entrySet())
            {
                tmp = entry1.getValue();
                v1+=(tmp*tmp);

                String word = entry1.getKey();
                if(eventsInfo.get(targetEvent).wordsCount.containsKey(word)){
                    numerator+=(keywordEvent.wordsCount.get(word)*eventsInfo.get(targetEvent).wordsCount.get(word));
                }
            }
            for(HashMap.Entry<String, Integer> entry2: eventsInfo.get(targetEvent).wordsCount.entrySet())
            {
                tmp = entry2.getValue();
                v2+=(tmp*tmp);
            }
            denominator = Math.sqrt(v1)*Math.sqrt(v2);
            double value_keyword = numerator/denominator;

            int rank_keyword = 0;
            int count = 0;

            it = testingEvents.iterator();
            while (it.hasNext()){
                if(count>=1000){
                    break;
                }
                int curr_event = it.next();
                if(testingUsers2testingEvents.get(curr_event).contains(user) && testingUsers2testingEvents.get(curr_event).contains(partner)){
                    continue;
                }

                double curr_value_keyword = calculateValueKeyword(targetEvent,curr_event);
                if(curr_value_keyword>=value_keyword){
                    rank_keyword++;
                }

                count++;
            }

            System.out.println("Keywordrank:"+rank_keyword);
            System.out.println("score:"+value_keyword);

            num_testcase++;
            System.out.println("num_testcase:"+num_testcase+"\n");

            if(num_testcase==5) {
                break;
            }
        }

        System.out.println("***********CASE END***********\n");
    }

    void KeywordAccuracyVer3() throws Exception{

        reset();

        String line;
        HashSet<Integer> testingEvents = new HashSet<>();
        HashSet<Integer> testingUsers = new HashSet<>();
        HashMap<Integer,HashSet<Integer>> testingUsers2testingEvents = new HashMap<>();

        BufferedReader bf = new BufferedReader(new FileReader("mem_testing.txt"));
        while ((line = bf.readLine()) != null) {
            String[] parts = line.split(",");
            int eventID = Integer.parseInt(parts[0]);
            if(!testingEvents.contains(eventID)){
                testingEvents.add(eventID);
                testingUsers2testingEvents.put(eventID,new HashSet<>());
                for(int i=1;i<parts.length;i++){
                    int userID = Integer.parseInt(parts[i]);
                    if(!testingUsers.contains(userID)){
                        testingUsers.add(userID);
                    }
                    testingUsers2testingEvents.get(eventID).add(userID);
                }
            }
        }
        bf.close();

        ArrayList<String> wordset = new ArrayList<>();
        HashSet<String> unique = new HashSet<>();
        Iterator<Integer> iterator = testingEvents.iterator();
        while(iterator.hasNext()) {
            int eventID = iterator.next();
            EventObject currEvent = eventsInfo.get(eventID);
            for(HashMap.Entry<String, Integer> entry: currEvent.wordsCount.entrySet())
            {
                String currWord = entry.getKey();
                if(!unique.contains(currWord)){
                    wordset.add(currWord);
                    unique.add(currWord);
                }
            }
        }

        int num_testcase = 0;
        iterator = testingEvents.iterator();
        while(iterator.hasNext()){

            int targetEvent = iterator.next();

            if(testingUsers2testingEvents.get(targetEvent).size()<2){
                continue;
            }

            Iterator<Integer> it = testingUsers2testingEvents.get(targetEvent).iterator();
            int user = it.next();
            int partner = it.next();

            EventObject keywordEvent = new EventObject();
            keywordEvent.wordsCount = new HashMap<>();
            Random random = new Random(num_testcase*1024);
            for(int i = 0;i<param.num_keyword;i++){
                String nextWord = wordset.get(random.nextInt(wordset.size()));
                if(keywordEvent.wordsCount.containsKey(nextWord)){
                    i--;
                }
                else {
                    keywordEvent.wordsCount.put(nextWord,1);
                }
            }

            double denominator;
            double numerator = 0;
            double v1=0,v2=0,tmp;
            for(HashMap.Entry<String, Integer> entry1: keywordEvent.wordsCount.entrySet())
            {
                tmp = entry1.getValue();
                v1+=(tmp*tmp);

                String word = entry1.getKey();
                if(eventsInfo.get(targetEvent).wordsCount.containsKey(word)){
                    numerator+=(keywordEvent.wordsCount.get(word)*eventsInfo.get(targetEvent).wordsCount.get(word));
                }
            }
            for(HashMap.Entry<String, Integer> entry2: eventsInfo.get(targetEvent).wordsCount.entrySet())
            {
                tmp = entry2.getValue();
                v2+=(tmp*tmp);
            }
            denominator = Math.sqrt(v1)*Math.sqrt(v2);
            double value_keyword = numerator/denominator;

            int rank_keyword = 0;
            int count = 0;

            it = testingEvents.iterator();
            while (it.hasNext()){
                if(count>=1000){
                    break;
                }
                int curr_event = it.next();
                if(testingUsers2testingEvents.get(curr_event).contains(user) && testingUsers2testingEvents.get(curr_event).contains(partner)){
                    continue;
                }

                double curr_value_keyword = calculateValueKeyword(targetEvent,curr_event);
                if(curr_value_keyword>=value_keyword){
                    rank_keyword++;
                }

                count++;
            }

            System.out.println("Keywordrank:"+rank_keyword);
            System.out.println("score:"+value_keyword);

            num_testcase++;
            System.out.println("num_testcase:"+num_testcase+"\n");

            if(num_testcase==10) {
                break;
            }
        }

        System.out.println("***********CASE END***********\n");
    }

    double calculateValueKeyword(int eid1,int eid2){
        EventObject o1 = eventsInfo.get(eid1);
        EventObject o2 = eventsInfo.get(eid2);
        double similarityValue = 0;
        double denominator = 0;
        double numerator = 0;
        double v1=0,v2=0,tmp;
        int count = 0;
        for(HashMap.Entry<String, Integer> entry1: o1.wordsCount.entrySet())
        {
            tmp = entry1.getValue();
            v1+=(tmp*tmp);

            String word = entry1.getKey();
            if(o2.wordsCount.containsKey(word)){
                numerator+=(o1.wordsCount.get(word)*o2.wordsCount.get(word));
            }

            count++;
            if (count>=this.param.num_keyword) {
                break;
            }
        }
        for(HashMap.Entry<String, Integer> entry2: o2.wordsCount.entrySet())
        {
            tmp = entry2.getValue();
            v2+=(tmp*tmp);
        }
        denominator = Math.sqrt(v1)*Math.sqrt(v2);

        similarityValue = numerator/denominator;
        return similarityValue;
    }


    void FNLJ(String query,int k,double alpha) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);
        
        this.param.queryTopK = k;
        this.param.textualWeight = alpha;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();

        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        long end = System.currentTimeMillis();
        this.calPre += (end-start);

        while (!nextEventQueue.isEmpty()){
            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                double kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            start = System.currentTimeMillis();
            HashSet<Integer> relatedEvents = getFilteredRelatedEvents(currEvent,totalEvents);
            end = System.currentTimeMillis();
            this.calN += (end-start);

            start = System.currentTimeMillis();
            FNLJPartnerComputation(currEvent,historicalPartners,relatedEvents);
            end = System.currentTimeMillis();
            this.calPartner += (end-start);

            TAtimes++;
            eventNumInvolved++;
        }
        round++;
        printTopkPairs("FNLJ.txt");
    }

    void FNLJPartnerComputation(EventObject currEvent, HashSet<Integer> partnerSet, HashSet<Integer> relatedEvents) throws Exception{

        if(partnerSet.size() == 0  || relatedEvents.size() == 0){
            return;
        }

        HashMap<Integer,Double> eventsSimilarity = new HashMap<>();
        double NDenominator = 0;

        Iterator<Integer> iterator = relatedEvents.iterator();
        while(iterator.hasNext()){
            int eventID = iterator.next();
            double textualSimilarity = calculateTextualSimilarity(currEvent,this.eventsInfo.get(eventID));
            eventsSimilarity.put(eventID,textualSimilarity);
            NDenominator+=(textualSimilarity);
        }

        double finalValue = 0;
        int finalPartner = -1;
        iterator = partnerSet.iterator();
        while (iterator.hasNext()){

            double currValue = 0;
            double left = 0;
            double right = 0;

            int currPartner = iterator.next();
            HashSet<Integer> events4currPartner = new HashSet<>(userToEvent.get(currPartner));
            events4currPartner.retainAll(attendedEvents);
            left = events4currPartner.size();

            events4currPartner = new HashSet<>(userToEvent.get(currPartner));
            events4currPartner.retainAll(relatedEvents);

            Iterator<Integer> it = events4currPartner.iterator();
            while(it.hasNext()){
                int eventID = it.next();
                right+=eventsSimilarity.get(eventID);
            }

            currValue=(left/attendedEvents.size())*(right/NDenominator);

            if(currValue>finalValue){
                finalValue = currValue;
                finalPartner = currPartner;
            }

            userNumInvolved++;
        }

        if(finalPartner != -1) {
            double FValue = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*finalValue;
            topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
        }
    }

    void FNLJ1(String query,int k,double alpha) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);


        this.param.queryTopK = k;
        this.param.textualWeight = alpha;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();

        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        long end = System.currentTimeMillis();
        this.calPre += (end-start);

        ArrayList<Pair> eventNum4partner = new ArrayList<>();
        iterator = historicalPartners.iterator();
        while (iterator.hasNext()) {
            int currPartner = (Integer) iterator.next();
            int currEventNum = userToEvent.get(currPartner).size();
            eventNum4partner.add(new Pair(-1,currPartner,currEventNum));
        }
        PairNumComparator customComparator = new PairNumComparator();
        eventNum4partner.sort(customComparator);

        while (!nextEventQueue.isEmpty()){
            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            start = System.currentTimeMillis();
            HashSet<Integer> relatedEvents = getFilteredRelatedEvents(currEvent,totalEvents);
            end = System.currentTimeMillis();
            this.calN += (end-start);

            start = System.currentTimeMillis();
            FNLJ1PartnerComputation(currEvent,historicalPartners,relatedEvents,kthValue,eventNum4partner);
            end = System.currentTimeMillis();
            this.calPartner += (end-start);

            TAtimes++;
            eventNumInvolved++;
        }
        round++;
        printTopkPairs("FNLJ1.txt");
    }

    void FNLJ1PartnerComputation(EventObject currEvent, HashSet<Integer> partnerSet, HashSet<Integer> relatedEvents,double kthValue,ArrayList<Pair> eventNum4partner) throws Exception{

        if(partnerSet.size() == 0  || relatedEvents.size() == 0){
            return;
        }

        HashMap<Integer,Double> eventsSimilarity = new HashMap<>();
        double NDenominator = 0;
        ArrayList<Double> NSimilarityList = new ArrayList<>();

        Iterator<Integer> iterator = relatedEvents.iterator();
        while(iterator.hasNext()){
            int eventID = iterator.next();
            double textualSimilarity = calculateTextualSimilarity(currEvent,this.eventsInfo.get(eventID));
            eventsSimilarity.put(eventID,textualSimilarity);
            NSimilarityList.add(textualSimilarity);
            NDenominator+=(textualSimilarity);
        }

        DoubleComparator doubleComparator = new DoubleComparator();
        NSimilarityList.sort(doubleComparator);

        ArrayList<Double>  NSimilaritySumList = new ArrayList<>();
        for (int i=0;i<NSimilarityList.size();i++){
            if(i==0){
                NSimilaritySumList.add(NSimilarityList.get(i));
            }
            else {
                NSimilaritySumList.add((NSimilaritySumList.get(i-1)+NSimilarityList.get(i)));
            }
        }

        double textual_similarity = calculateQueryTextualSimilarity(param.queryObject,currEvent);
        double pw = (kthValue-param.textualWeight*textual_similarity)/(1-param.textualWeight);
        int maxPartner = eventNum4partner.get(0).partnerID;
        int maxNum1NStar = Math.min(attendedEvents.size(),userToEvent.get(maxPartner).size());
        int maxNum1N = Math.min(relatedEvents.size(),userToEvent.get(maxPartner).size());
        double pb = NSimilaritySumList.get(maxNum1N-1);
        pb = (pb/NDenominator)*(maxNum1NStar/(double)attendedEvents.size());
        if(pw >= pb){
            eventNumPruning++;
            eventNumPruningByOpt1++;
            return;
        }

        double finalValue = 0;
        int finalPartner = -1;
        iterator = partnerSet.iterator();
        while (iterator.hasNext()){

            double currValue = 0;
            double left = 0;
            double right = 0;

            int currPartner = iterator.next();
            HashSet<Integer> events4currPartner = new HashSet<>(userToEvent.get(currPartner));
            events4currPartner.retainAll(attendedEvents);
            left = events4currPartner.size();

            events4currPartner = new HashSet<>(userToEvent.get(currPartner));
            events4currPartner.retainAll(relatedEvents);

            Iterator<Integer> it = events4currPartner.iterator();
            while(it.hasNext()){
                int eventID = it.next();
                right+=eventsSimilarity.get(eventID);
            }

            currValue=(left/attendedEvents.size())*(right/NDenominator);

            if(currValue>finalValue){
                finalValue = currValue;
                finalPartner = currPartner;
            }

            userNumInvolved++;
        }

        if(finalPartner != -1) {
            double FValue = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*finalValue;
            topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
        }
    }

    void FNLJ2(String query,int k,double alpha) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);


        this.param.queryTopK = k;
        this.param.textualWeight = alpha;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();

        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        long end = System.currentTimeMillis();
        this.calPre += (end-start);

        ArrayList<Pair> eventNum4partner = new ArrayList<>();
        iterator = historicalPartners.iterator();
        while (iterator.hasNext()) {
            int currPartner = (Integer) iterator.next();
            int currEventNum = userToEvent.get(currPartner).size();
            eventNum4partner.add(new Pair(-1,currPartner,currEventNum));
        }
        PairNumComparator customComparator = new PairNumComparator();
        eventNum4partner.sort(customComparator);

        while (!nextEventQueue.isEmpty()){
            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            start = System.currentTimeMillis();
            HashSet<Integer> relatedEvents = getFilteredRelatedEvents(currEvent,totalEvents);
            end = System.currentTimeMillis();
            this.calN += (end-start);

            start = System.currentTimeMillis();
            FNLJ2PartnerComputation(currEvent,historicalPartners,relatedEvents,kthValue,eventNum4partner);
            end = System.currentTimeMillis();
            this.calPartner += (end-start);

            TAtimes++;
            eventNumInvolved++;
        }
        round++;
        printTopkPairs("FNLJ2.txt");
    }

    HashSet<Integer> getHistoricalPartners(){
        HashSet<Integer> historicalPartners = new HashSet<>();
        for(int i=0;i<attendedEvents.size();i++){
            historicalPartners.addAll(eventToUsers.get(attendedEvents.get(i)));
        }
        if(historicalPartners.contains(param.queryUserID)){
            historicalPartners.remove(param.queryUserID);
        }
        return historicalPartners;
    }

    void generateNextEventsQueueWithII(EventObject queryObject){

        double tmpMaxTS = 0;

        HashSet<Integer> candidateEvents = getRelatedEventFromInvertedIndex(queryObject);
        Iterator<Integer> iterator = candidateEvents.iterator();
        while(iterator.hasNext()){
            EventObject currEvent = eventsInfo.get(iterator.next());
            double textualSimilarity = calculateQueryTextualSimilarity(queryObject,currEvent);
            if(tmpMaxTS<textualSimilarity){
                tmpMaxTS = textualSimilarity;
            }
            double value = param.textualWeight*textualSimilarity;
            this.nextEventQueue.add(new Pair(currEvent.eventID,0,value));
        }
        factor = tmpMaxTS;
    }

    HashSet<Integer> getFilteredRelatedEvents(EventObject currEvent,HashSet<Integer> totalEvents){

        HashSet<Integer> result = new HashSet<>();
        ArrayList<Integer> candidateEvents;

        for (HashMap.Entry<String, Integer> entry : currEvent.wordsCount.entrySet()) {
            String currWord = entry.getKey();
            candidateEvents = InvertedIndex.get(currWord);
            for(int i=0;i<candidateEvents.size();i++){
                int curr = candidateEvents.get(i);
                if(totalEvents.contains(curr) && isNeighborhood(currEvent,eventsInfo.get(curr))){
                    result.add(curr);
                }
            }
        }

        return result;
    }

    void FNLJ2PartnerComputation(EventObject currEvent, HashSet<Integer> partnerSet, HashSet<Integer> relatedEvents,double kthValue,ArrayList<Pair> eventNum4partner) throws Exception{

        if(partnerSet.size() == 0 || relatedEvents.size() == 0){
            return;
        }

        ArrayList<Double> NSimilarityList = new ArrayList<>();

        HashMap<Integer,Double> eventsSimilarity = new HashMap<>();

        double NDenominator = 0;

        Iterator iterator = relatedEvents.iterator();
        while(iterator.hasNext()){
            int eventID = (Integer) iterator.next();
            double textualSimilarity = calculateTextualSimilarity(currEvent,eventsInfo.get(eventID));
            eventsSimilarity.put(eventID,textualSimilarity);
            NSimilarityList.add(textualSimilarity);
            NDenominator+=(textualSimilarity);
        }

        DoubleComparator doubleComparator = new DoubleComparator();
        NSimilarityList.sort(doubleComparator);

        ArrayList<Double>  NSimilaritySumList = new ArrayList<>();
        for (int i=0;i<NSimilarityList.size();i++){
            if(i==0){
                NSimilaritySumList.add(NSimilarityList.get(i));
            }
            else {
                NSimilaritySumList.add((NSimilaritySumList.get(i-1)+NSimilarityList.get(i)));
            }
        }

        double textual_similarity = calculateQueryTextualSimilarity(param.queryObject,currEvent);

        double pv = 0;
        int finalPartner = -1;
        for(int i = 0; i < eventNum4partner.size(); i++){
            if(i>0){
                int curr1 = Math.min(relatedEvents.size(),userToEvent.get(eventNum4partner.get(i).partnerID).size());
                int curr2 = Math.min(attendedEvents.size(),userToEvent.get(eventNum4partner.get(i).partnerID).size());
                double tmpMax = (NSimilaritySumList.get(curr1-1)/NDenominator)*(curr2/(double)attendedEvents.size());
                if(pv >= tmpMax){
                    eventNumPruning++;
                    if(finalPartner != -1){
                        double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
                    }
                    return;
                }
            }

            double currValue;
            double left;
            double right = 0;

            int currMaxPartner = eventNum4partner.get(i).partnerID;
            HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(attendedEvents);
            left = events4currMaxPartner.size();

            events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(relatedEvents);
            iterator = events4currMaxPartner.iterator();
            while(iterator.hasNext()){
                int eventID = (Integer) iterator.next();
                right+=eventsSimilarity.get(eventID);
            }

            currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
            if(currValue>pv){
                pv = currValue;
                finalPartner = eventNum4partner.get(i).partnerID;
            }

            userNumInvolved++;
        }

        if(finalPartner != -1){
            double FValue = param.textualWeight*textual_similarity + param.togetherWeight*pv;
            topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
        }
    }

    void FNLJ3(String query,int k,double alpha) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);


        this.param.queryTopK = k;
        this.param.textualWeight = alpha;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();

        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        long end = System.currentTimeMillis();
        this.calPre += (end-start);

        start = System.currentTimeMillis();
        HashMap<Integer,Integer> auxiliaryU2R = new HashMap<>();
        HashMap<Integer,HashSet<Integer>> auxiliaryR2Us = new HashMap<>();
        ArrayList<Integer> ROrder = new ArrayList<>();
        if(userToEvent.containsKey(param.queryUserID)){
            iterator = historicalPartners.iterator();
            while (iterator.hasNext()){
                int currUser = iterator.next();
                if(userToEvent.containsKey(currUser)){
                    HashSet<Integer> tmp = new HashSet<>(userToEvent.get(param.queryUserID));
                    tmp.retainAll(userToEvent.get(currUser));
                    int R = tmp.size();
                    auxiliaryU2R.put(currUser,R);
                    if(!auxiliaryR2Us.containsKey(R)){
                        auxiliaryR2Us.put(R,new HashSet<>());
                        ROrder.add(R);
                    }
                    auxiliaryR2Us.get(R).add(currUser);
                }
            }
        }
        IntegerComparator ic = new IntegerComparator();
        ROrder.sort(ic);
        end = System.currentTimeMillis();
        this.cal1 += (end-start);

        start = System.currentTimeMillis();
        HashMap<Integer,Integer> auxiliaryU2E = new HashMap<>();
        HashMap<Integer,HashSet<Integer>> auxiliaryE2Us = new HashMap<>();
        ArrayList<Integer> EOrder = new ArrayList<>();
        iterator = historicalPartners.iterator();
        while (iterator.hasNext()){
            int currUser = iterator.next();
            if(userToEvent.containsKey(currUser)){
                int E = userToEvent.get(currUser).size();
                auxiliaryU2E.put(currUser,E);
                if(!auxiliaryE2Us.containsKey(E)){
                    auxiliaryE2Us.put(E,new HashSet<>());
                    EOrder.add(E);
                }
                auxiliaryE2Us.get(E).add(currUser);
            }
        }
        EOrder.sort(ic);

        ArrayList<Pair> eventNum4partner = new ArrayList<>();
        for(HashMap.Entry<Integer, Integer> entry: auxiliaryU2E.entrySet())
        {
            int currUser = entry.getKey();
            int currE = auxiliaryU2E.get(currUser);
            int currR = auxiliaryU2R.get(currUser);
            eventNum4partner.add(new Pair(-1,currUser,currE*currR));
        }
        PairNumComparator customComparator = new PairNumComparator();
        eventNum4partner.sort(customComparator);

        end = System.currentTimeMillis();
        this.cal2 += (end-start);

        while (!nextEventQueue.isEmpty()){
            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            start = System.currentTimeMillis();
            HashSet<Integer> relatedEvents = getFilteredRelatedEvents(currEvent,totalEvents);
            end = System.currentTimeMillis();
            this.calN += (end-start);

            start = System.currentTimeMillis();
            FNLJ3PartnerComputation(currEvent,historicalPartners,relatedEvents,kthValue,ROrder,auxiliaryU2R,auxiliaryR2Us,EOrder,auxiliaryU2E,auxiliaryE2Us,eventNum4partner);
            end = System.currentTimeMillis();
            this.calPartner += (end-start);

            TAtimes++;
            eventNumInvolved++;
        }
        round++;
        printTopkPairs("FNLJ3.txt");
    }

    void FNLJ3PartnerComputation(EventObject currEvent, HashSet<Integer> partnerSet, HashSet<Integer> relatedEvents,double kthValue,ArrayList<Integer> ROrder,HashMap<Integer,Integer> auxiliaryU2R,HashMap<Integer,HashSet<Integer>> auxiliaryR2Us,ArrayList<Integer> EOrder,HashMap<Integer,Integer> auxiliaryU2E,HashMap<Integer,HashSet<Integer>> auxiliaryE2Us,ArrayList<Pair> eventNum4partner) throws Exception{

        if(partnerSet.size() == 0 || relatedEvents.size() == 0){
            return;
        }

        ArrayList<Double> NSimilarityList = new ArrayList<>();

        HashMap<Integer,Double> eventsSimilarity = new HashMap<>();

        double NDenominator = 0;

        Iterator iterator = relatedEvents.iterator();
        while(iterator.hasNext()){
            int eventID = (Integer) iterator.next();
            double textualSimilarity = calculateTextualSimilarity(currEvent,eventsInfo.get(eventID));
            eventsSimilarity.put(eventID,textualSimilarity);
            NSimilarityList.add(textualSimilarity);
            NDenominator+=(textualSimilarity);
        }

        DoubleComparator doubleComparator = new DoubleComparator();
        NSimilarityList.sort(doubleComparator);

        ArrayList<Double>  NSimilaritySumList = new ArrayList<>();
        for (int i=0;i<NSimilarityList.size();i++){
            if(i==0){
                NSimilaritySumList.add(NSimilarityList.get(i));
            }
            else {
                NSimilaritySumList.add((NSimilaritySumList.get(i-1)+NSimilarityList.get(i)));
            }
        }

        double textual_similarity = calculateQueryTextualSimilarity(param.queryObject,currEvent);

        double pv = 0;
        int finalPartner = -1;

        int k1 = 0;
        int k2 = 0;
        HashMap<Integer,Integer> RCount = new HashMap<>();
        HashMap<Integer,Integer> ECount = new HashMap<>();

        for(int i = 0; i < eventNum4partner.size(); i++){
            if(i>0){
                int curr1 = 0;
                if(EOrder.size() > k2){
                    int currE = EOrder.get(k2);
                    curr1 = Math.min(relatedEvents.size(),currE);
                    while (ECount.containsKey(currE) && auxiliaryE2Us.get(currE).size() == ECount.get(currE)){
                        k2++;
                        if(EOrder.size() <= k2){
                            curr1 = 0;
                            break;
                        }
                        currE = EOrder.get(k2);
                        curr1 = Math.min(relatedEvents.size(),currE);
                    }
                }

                int curr2 = 0;
                if(ROrder.size() > k1){
                    int currR = ROrder.get(k1);
                    curr2 = currR;
                    while (RCount.containsKey(currR) && auxiliaryR2Us.get(currR).size() == RCount.get(currR)){
                        k1++;
                        if(ROrder.size() <= k1){
                            curr2 = 0;
                            break;
                        }
                        currR = ROrder.get(k1);
                        curr2 = currR;
                    }
                }

                double tmpMax = (NSimilaritySumList.get(curr1-1)/NDenominator)*(curr2/(double)attendedEvents.size());
                if(pv >= tmpMax){
                    eventNumPruning++;
                    if(finalPartner != -1){
                        double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
                    }
                    return;
                }
            }

            double currValue;
            double left;
            double right = 0;

            int currMaxPartner = eventNum4partner.get(i).partnerID;
            HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(attendedEvents);
            left = events4currMaxPartner.size();

            events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(relatedEvents);
            iterator = events4currMaxPartner.iterator();
            while(iterator.hasNext()){
                int eventID = (Integer) iterator.next();
                right+=eventsSimilarity.get(eventID);
            }

            currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
            if(currValue>pv){
                pv = currValue;
                finalPartner = currMaxPartner;
            }

            if(auxiliaryU2R.containsKey(currMaxPartner)){
                int currR = auxiliaryU2R.get(currMaxPartner);
                if(!RCount.containsKey(currR)){
                    RCount.put(currR,1);
                }
                else {
                    int tmp = RCount.get(currR);
                    RCount.remove(currR);
                    RCount.put(currR,tmp+1);
                }
            }
            if(auxiliaryU2E.containsKey(currMaxPartner)){
                int currE = auxiliaryU2E.get(currMaxPartner);
                if(!ECount.containsKey(currE)){
                    ECount.put(currE,1);
                }
                else {
                    int tmp = ECount.get(currE);
                    ECount.remove(currE);
                    ECount.put(currE,tmp+1);
                }
            }
            userNumInvolved++;
        }

        if(finalPartner != -1){
            double FValue = param.textualWeight*textual_similarity + param.togetherWeight*pv;
            topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
        }
    }

    void FNLJStar(String query,int k,double alpha) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);


        this.param.queryTopK = k;
        this.param.textualWeight = alpha;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();
        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        HashMap<Integer,HashSet<Integer>> historicalPartnerInRelatedEvents = new HashMap<>();
        iterator = totalEvents.iterator();
        while(iterator.hasNext()){
            int currE = iterator.next();
            HashSet<Integer> currU = eventToUsers.get(currE);
            Iterator<Integer> it = currU.iterator();
            HashSet<Integer> historicalPartnerInSingleRelatedEvents = new HashSet<>();
            while (it.hasNext()){
                int user = it.next();
                if(historicalPartners.contains(user)){
                    historicalPartnerInSingleRelatedEvents.add(user);
                }
            }
            historicalPartnerInRelatedEvents.put(currE,historicalPartnerInSingleRelatedEvents);
        }
        long end = System.currentTimeMillis();
        this.calPre += (end-start);

        start = System.currentTimeMillis();
        HashMap<Integer,Integer> auxiliaryU2R = new HashMap<>();
        HashMap<Integer,HashSet<Integer>> auxiliaryR2Us = new HashMap<>();
        ArrayList<Integer> ROrder = new ArrayList<>();
        if(userToEvent.containsKey(param.queryUserID)){
            iterator = historicalPartners.iterator();
            while (iterator.hasNext()){
                int currUser = iterator.next();
                if(userToEvent.containsKey(currUser)){
                    HashSet<Integer> tmp = new HashSet<>(userToEvent.get(param.queryUserID));
                    tmp.retainAll(userToEvent.get(currUser));
                    int R = tmp.size();
                    auxiliaryU2R.put(currUser,R);
                    if(!auxiliaryR2Us.containsKey(R)){
                        auxiliaryR2Us.put(R,new HashSet<>());
                        ROrder.add(R);
                    }
                    auxiliaryR2Us.get(R).add(currUser);
                }
            }
        }
        IntegerComparator ic = new IntegerComparator();
        ROrder.sort(ic);
        end = System.currentTimeMillis();
        this.cal1 += (end-start);

        start = System.currentTimeMillis();
        HashMap<Integer,Integer> auxiliaryU2E = new HashMap<>();
        HashMap<Integer,HashSet<Integer>> auxiliaryE2Us = new HashMap<>();
        ArrayList<Integer> EOrder = new ArrayList<>();
        iterator = historicalPartners.iterator();
        while (iterator.hasNext()){
            int currUser = iterator.next();
            if(userToEvent.containsKey(currUser)){
                int E = userToEvent.get(currUser).size();
                auxiliaryU2E.put(currUser,E);
                if(!auxiliaryE2Us.containsKey(E)){
                    auxiliaryE2Us.put(E,new HashSet<>());
                    EOrder.add(E);
                }
                auxiliaryE2Us.get(E).add(currUser);
            }
        }
        EOrder.sort(ic);

        ArrayList<Pair> eventNum4partner = new ArrayList<>();
        for(HashMap.Entry<Integer, Integer> entry: auxiliaryU2E.entrySet())
        {
            int currUser = entry.getKey();
            int currE = auxiliaryU2E.get(currUser);
            int currR = auxiliaryU2R.get(currUser);
            eventNum4partner.add(new Pair(-1,currUser,currE*currR));
        }
        PairNumComparator customComparator = new PairNumComparator();
        eventNum4partner.sort(customComparator);

        end = System.currentTimeMillis();
        this.cal2 += (end-start);

        while (!nextEventQueue.isEmpty()){
            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            start = System.currentTimeMillis();
            HashSet<Integer> relatedEvents = new HashSet<>();
            HashSet<Integer> presentU = new HashSet<>();
            ArrayList<Integer> candidateEvents;
            for (HashMap.Entry<String, Integer> entry : currEvent.wordsCount.entrySet()) {
                String currWord = entry.getKey();
                candidateEvents = InvertedIndex.get(currWord);
                for(int i=0;i<candidateEvents.size();i++){
                    int curr = candidateEvents.get(i);
                    if(totalEvents.contains(curr) && isNeighborhood(currEvent,eventsInfo.get(curr))){
                        relatedEvents.add(curr);
                        if(historicalPartnerInRelatedEvents.containsKey(curr)){
                            presentU.addAll(historicalPartnerInRelatedEvents.get(curr));
                        }
                    }
                }
            }
            end = System.currentTimeMillis();
            this.calN += (end-start);

            start = System.currentTimeMillis();
            HashMap<Integer,Integer> RCount = new HashMap<>();
            HashMap<Integer,Integer> ECount = new HashMap<>();
            iterator = presentU.iterator();
            while (iterator.hasNext()) {
                int currU = iterator.next();
                if(auxiliaryU2R.containsKey(currU)){
                    int currR = auxiliaryU2R.get(currU);
                    if(!RCount.containsKey(currR)){
                        RCount.put(currR,1);
                    }
                    else {
                        int tmp = RCount.get(currR);
                        RCount.remove(currR);
                        RCount.put(currR,tmp+1);
                    }
                }
                if(auxiliaryU2E.containsKey(currU)){
                    int currE = auxiliaryU2E.get(currU);
                    if(!ECount.containsKey(currE)){
                        ECount.put(currE,1);
                    }
                    else {
                        int tmp = ECount.get(currE);
                        ECount.remove(currE);
                        ECount.put(currE,tmp+1);
                    }
                }
            }
            end = System.currentTimeMillis();
            this.calU += (end-start);

            start = System.currentTimeMillis();
            FNLJStarPartnerComputation(currEvent,historicalPartners,relatedEvents,kthValue,ROrder,auxiliaryU2R,auxiliaryR2Us,EOrder,auxiliaryU2E,auxiliaryE2Us,eventNum4partner,RCount,ECount,presentU);
            end = System.currentTimeMillis();
            this.calPartner += (end-start);

            TAtimes++;
            eventNumInvolved++;
        }
        round++;
        printTopkPairs("FNLJStar.txt");
    }

    void FNLJStarPartnerComputation(EventObject currEvent, HashSet<Integer> partnerSet, HashSet<Integer> relatedEvents,double kthValue,ArrayList<Integer> ROrder,HashMap<Integer,Integer> auxiliaryU2R,HashMap<Integer,HashSet<Integer>> auxiliaryR2Us,ArrayList<Integer> EOrder,HashMap<Integer,Integer> auxiliaryU2E,HashMap<Integer,HashSet<Integer>> auxiliaryE2Us,ArrayList<Pair> eventNum4partner,HashMap<Integer,Integer> RCount,HashMap<Integer,Integer> ECount,HashSet<Integer> presentU) throws Exception{

        if(partnerSet.size() == 0 || relatedEvents.size() == 0){
            return;
        }

        ArrayList<Double> NSimilarityList = new ArrayList<>();

        HashMap<Integer,Double> eventsSimilarity = new HashMap<>();

        double NDenominator = 0;

        Iterator iterator = relatedEvents.iterator();
        while(iterator.hasNext()){
            int eventID = (Integer) iterator.next();
            double textualSimilarity = calculateTextualSimilarity(currEvent,eventsInfo.get(eventID));
            eventsSimilarity.put(eventID,textualSimilarity);
            NSimilarityList.add(textualSimilarity);
            NDenominator+=(textualSimilarity);
        }

        DoubleComparator doubleComparator = new DoubleComparator();
        NSimilarityList.sort(doubleComparator);

        ArrayList<Double>  NSimilaritySumList = new ArrayList<>();
        for (int i=0;i<NSimilarityList.size();i++){
            if(i==0){
                NSimilaritySumList.add(NSimilarityList.get(i));
            }
            else {
                NSimilaritySumList.add((NSimilaritySumList.get(i-1)+NSimilarityList.get(i)));
            }
        }

        double textual_similarity = calculateQueryTextualSimilarity(param.queryObject,currEvent);
        double pw = (kthValue-param.textualWeight*textual_similarity)/(1-param.textualWeight);
        int maxNum1NStar = 0;
        int maxNum1N = 0;
        int index = 0;
        while (ROrder.size()>index && !RCount.containsKey(ROrder.get(index))){
            index++;
        }
        if(ROrder.size()>index){
            maxNum1NStar = ROrder.get(index);
        }
        index = 0;
        while (EOrder.size()>index && !ECount.containsKey(EOrder.get(index))){
            index++;
        }
        if(EOrder.size()>index){
            maxNum1N = Math.min(relatedEvents.size(),EOrder.get(index));
        }
        double pb = 0;
        if(maxNum1N != 0){
            pb = NSimilaritySumList.get(maxNum1N-1);
        }
        pb = (pb/NDenominator)*(maxNum1NStar/(double)attendedEvents.size());
        if(pw >= pb){
            eventNumPruning++;
            eventNumPruningByOpt1++;
            return;
        }

        double pv = 0;
        int finalPartner = -1;

        int k1 = 0;
        int k2 = 0;

        for(int i = 0; i < eventNum4partner.size(); i++){

            int currMaxPartner = eventNum4partner.get(i).partnerID;
            if(!presentU.contains(currMaxPartner)){
                continue;
            }

            if(i>0){
                int curr1 = 0;
                if(EOrder.size() > k2){
                    int currE = EOrder.get(k2);
                    curr1 = Math.min(relatedEvents.size(),currE);
                    while (!ECount.containsKey(currE) || ECount.get(currE) == 0){
                        k2++;
                        if(EOrder.size() <= k2){
                            curr1 = 0;
                            break;
                        }
                        currE = EOrder.get(k2);
                        curr1 = Math.min(relatedEvents.size(),currE);
                    }
                }

                int curr2 = 0;
                if(ROrder.size() > k1){
                    int currR = ROrder.get(k1);
                    curr2 = currR;
                    while (!RCount.containsKey(currR) || RCount.get(currR) == 0){
                        k1++;
                        if(ROrder.size() <= k1){
                            curr2 = 0;
                            break;
                        }
                        currR = ROrder.get(k1);
                        curr2 = currR;
                    }
                }

                double tmpMax = 0;
                if(curr1 != 0 && curr2 != 0){
                    tmpMax = (NSimilaritySumList.get(curr1-1)/NDenominator)*(curr2/(double)attendedEvents.size());
                }
                if (pw >= tmpMax) {
                    eventNumPruning++;
                    if (finalPartner != -1) {
                        double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent.eventID, finalPartner, FValue));
                    }
                    return;
                }
                if(pv >= tmpMax){
                    eventNumPruning++;
                    if(finalPartner != -1){
                        double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
                    }
                    return;
                }
            }

            double currValue;
            double left;
            double right = 0;

            HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(attendedEvents);
            left = events4currMaxPartner.size();

            events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(relatedEvents);
            iterator = events4currMaxPartner.iterator();
            while(iterator.hasNext()){
                int eventID = (Integer) iterator.next();
                right+=eventsSimilarity.get(eventID);
            }

            currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
            if(currValue>pv){
                pv = currValue;
                finalPartner = currMaxPartner;
            }

            if(auxiliaryU2R.containsKey(currMaxPartner)){
                int currR = auxiliaryU2R.get(currMaxPartner);
                int tmp = RCount.get(currR);
                RCount.remove(currR);
                RCount.put(currR,tmp-1);
            }
            if(auxiliaryU2E.containsKey(currMaxPartner)){
                int currE = auxiliaryU2E.get(currMaxPartner);
                int tmp = ECount.get(currE);
                ECount.remove(currE);
                ECount.put(currE,tmp-1);
            }
            userNumInvolved++;
        }

        if(finalPartner != -1){
            double FValue = param.textualWeight*textual_similarity + param.togetherWeight*pv;
            topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
        }
    }

    void FRJ(String query,int k,double alpha,int r) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);

        this.param.queryTopK = k;
        this.param.textualWeight = alpha;
        this.augment = r;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();

        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        long end = System.currentTimeMillis();
        this.calPre += (end-start);

        ArrayList<Pair> eventNum4partner = new ArrayList<>();
        iterator = historicalPartners.iterator();
        while (iterator.hasNext()) {
            int currPartner = (Integer) iterator.next();
            int currEventNum = userToEvent.get(currPartner).size();
            eventNum4partner.add(new Pair(-1,currPartner,currEventNum));
        }
        PairNumComparator customComparator = new PairNumComparator();
        eventNum4partner.sort(customComparator);

        while (!nextEventQueue.isEmpty()){
            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            start = System.currentTimeMillis();
            HashSet<Integer> relatedEvents = getFilteredRelatedEvents(currEvent,totalEvents);
            end = System.currentTimeMillis();
            this.calN += (end-start);

            start = System.currentTimeMillis();
            int duplicate = currEvent.eventID;
            FRJPartnerComputation(currEvent,historicalPartners,relatedEvents,kthValue,eventNum4partner);
            FRJRJProcess(eventNum4partner,duplicate,kthValue);
            step+=augment;
            end = System.currentTimeMillis();
            this.calPartner += (end-start);

            TAtimes++;
            eventNumInvolved++;
        }

        start = System.currentTimeMillis();
        int duplicate = -1;
        double kthValue = 0;
        if(topkEventPartnerQueue.size()>=param.queryTopK){
            kthValue = calculateTopKthValue();
        }
        while (step < eventNum4partner.size()){
            FRJRJProcess(eventNum4partner,duplicate,kthValue);
            step+=augment;
        }
        end = System.currentTimeMillis();
        this.calPartner += (end-start);

        for(HashMap.Entry<Integer,Pair> entry:mapTempEP.entrySet()){
            int currEvent = entry.getKey();
            Pair currPair = entry.getValue();
            double currValue = param.textualWeight * calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent)) + param.togetherWeight * currPair.score;
            topkEventPartnerQueue.add(new Pair(currEvent, currPair.partnerID, currValue));
        }

        round++;
        printTopkPairs("FRJ.txt");
    }

    void FRJPartnerComputation(EventObject currEvent, HashSet<Integer> partnerSet, HashSet<Integer> relatedEvents,double kthValue,ArrayList<Pair> eventNum4partner){

        if(partnerSet.size() == 0 || relatedEvents.size() == 0){
            return;
        }

        ArrayList<Double> NSimilarityList = new ArrayList<>();

        HashMap<Integer,Double> eventsSimilarity = new HashMap<>();

        double NDenominator = 0;

        Iterator iterator = relatedEvents.iterator();
        while(iterator.hasNext()){
            int eventID = (Integer) iterator.next();
            double textualSimilarity = calculateTextualSimilarity(currEvent,eventsInfo.get(eventID));
            eventsSimilarity.put(eventID,textualSimilarity);
            NSimilarityList.add(textualSimilarity);
            NDenominator+=(textualSimilarity);
        }

        DoubleComparator doubleComparator = new DoubleComparator();
        NSimilarityList.sort(doubleComparator);

        ArrayList<Double>  NSimilaritySumList = new ArrayList<>();
        for (int i=0;i<NSimilarityList.size();i++){
            if(i==0){
                NSimilaritySumList.add(NSimilarityList.get(i));
            }
            else {
                NSimilaritySumList.add((NSimilaritySumList.get(i-1)+NSimilarityList.get(i)));
            }
        }

        double textual_similarity = calculateQueryTextualSimilarity(param.queryObject,currEvent);
        int maxPartner = eventNum4partner.get(0).partnerID;
        int maxNum1NStar = Math.min(attendedEvents.size(),userToEvent.get(maxPartner).size());
        int maxNum1N = Math.min(relatedEvents.size(),userToEvent.get(maxPartner).size());
        double pb = NSimilaritySumList.get(maxNum1N-1);
        pb = (pb/NDenominator)*(maxNum1NStar/(double)attendedEvents.size());

        double tempTopk = 0;
        if(mapTempEP.size()>=param.queryTopK){
            tempTopk = FRJTempTopKth();
        }
        if(tempTopk >= pb){
            eventNumPruning++;
            return;
        }

        double pv = 0;
        int finalPartner = -1;

        for(int i = 0; i < eventNum4partner.size() && i<step; i++){

            int currMaxPartner = eventNum4partner.get(i).partnerID;

            double currValue;
            double left;
            double right = 0;

            HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(attendedEvents);
            left = events4currMaxPartner.size();

            events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(relatedEvents);
            iterator = events4currMaxPartner.iterator();
            while(iterator.hasNext()){
                int eventID = (Integer) iterator.next();
                right+=eventsSimilarity.get(eventID);
            }

            currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
            if(currValue>pv){
                pv = currValue;
                finalPartner = currMaxPartner;
            }

            userNumInvolved++;
        }

        if(step>=eventNum4partner.size()){
            if (finalPartner != -1) {
                double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                topkEventPartnerQueue.add(new Pair(currEvent.eventID, finalPartner, FValue));
            }
        }
        else {
            mapTempEP.put(currEvent.eventID,new Pair(-1,finalPartner,pv));
            mapEventsSimilarity.put(currEvent.eventID, eventsSimilarity);
            mapNDenominator.put(currEvent.eventID, NDenominator);
            mapNSimilaritySumList.put(currEvent.eventID, NSimilaritySumList);
            mapRelatedEvents.put(currEvent.eventID, relatedEvents);
        }
    }

    void FRJRJProcess(ArrayList<Pair> eventNum4partner,int duplicate,double KthValue){

        ArrayList<Integer> removed = new ArrayList<>();

        double tempTopk = 0;
        if(mapTempEP.size()>=param.queryTopK){
            tempTopk = FRJTempTopKth();
        }

        for(HashMap.Entry<Integer,HashMap<Integer,Double>> entry:mapEventsSimilarity.entrySet()){

            int currEvent = entry.getKey();

            if(duplicate == currEvent){
                continue;
            }

            ArrayList<Double> NSimilaritySumList = mapNSimilaritySumList.get(currEvent);
            HashSet<Integer> relatedEvents = mapRelatedEvents.get(currEvent);
            Double NDenominator = mapNDenominator.get(currEvent);
            HashMap<Integer,Double> eventsSimilarity = mapEventsSimilarity.get(currEvent);

            double textualSimilarity = calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent));

            int maxPartner = eventNum4partner.get(eventNum4partner.size()-1).partnerID;
            if((step-augment)<eventNum4partner.size()){
                maxPartner = eventNum4partner.get(step-augment).partnerID;
            }
            int maxNum1NStar = Math.min(attendedEvents.size(),userToEvent.get(maxPartner).size());
            int maxNum1N = Math.min(relatedEvents.size(),userToEvent.get(maxPartner).size());
            double pb = NSimilaritySumList.get(maxNum1N-1);
            pb = (pb/NDenominator)*(maxNum1NStar/(double)attendedEvents.size());
            if(tempTopk >= pb){
                eventNumPruning++;
                Pair EP = mapTempEP.get(currEvent);
                if (EP.partnerID != -1) {
                    double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * EP.score;
                    topkEventPartnerQueue.add(new Pair(currEvent, EP.partnerID, FValue));
                }
                removed.add(currEvent);
                continue;
            }

            double pv = mapTempEP.get(currEvent).score;
            int finalPartner = mapTempEP.get(currEvent).partnerID;
            for(int i = step-augment; i < eventNum4partner.size() && i<step; i++){

                int currMaxPartner = eventNum4partner.get(i).partnerID;

                double currValue;
                double left;
                double right = 0;

                HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
                events4currMaxPartner.retainAll(attendedEvents);
                left = events4currMaxPartner.size();

                events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
                events4currMaxPartner.retainAll(relatedEvents);
                Iterator<Integer> iterator = events4currMaxPartner.iterator();
                while(iterator.hasNext()){
                    int eventID = iterator.next();
                    right+=eventsSimilarity.get(eventID);
                }

                currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
                if(currValue>pv){
                    pv = currValue;
                    finalPartner = currMaxPartner;
                }

                userNumInvolved++;
            }

            if(pv > mapTempEP.get(currEvent).score){
                mapTempEP.remove(currEvent);
                mapTempEP.put(currEvent,new Pair(-1,finalPartner,pv));
            }

            if(step>=eventNum4partner.size()){
                Pair EP = mapTempEP.get(currEvent);
                if (EP.partnerID != -1) {
                    double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * EP.score;
                    topkEventPartnerQueue.add(new Pair(currEvent, EP.partnerID, FValue));
                }
                removed.add(currEvent);
            }
        }

        for(int i = 0;i<removed.size();i++){
            mapEventsSimilarity.remove(removed.get(i));
            mapNDenominator.remove(removed.get(i));
            mapNSimilaritySumList.remove(removed.get(i));
            mapRelatedEvents.remove(removed.get(i));
            mapTempEP.remove(removed.get(i));
        }
    }

    void FRJ1(String query,int k,double alpha,int r) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);

        this.param.queryTopK = k;
        this.param.textualWeight = alpha;
        this.augment = r;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();

        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        long end = System.currentTimeMillis();
        this.calPre += (end-start);

        ArrayList<Pair> eventNum4partner = new ArrayList<>();
        iterator = historicalPartners.iterator();
        while (iterator.hasNext()) {
            int currPartner = (Integer) iterator.next();
            int currEventNum = userToEvent.get(currPartner).size();
            eventNum4partner.add(new Pair(-1,currPartner,currEventNum));
        }
        PairNumComparator customComparator = new PairNumComparator();
        eventNum4partner.sort(customComparator);

        while (!nextEventQueue.isEmpty()){
            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            start = System.currentTimeMillis();
            HashSet<Integer> relatedEvents = getFilteredRelatedEvents(currEvent,totalEvents);
            end = System.currentTimeMillis();
            this.calN += (end-start);

            start = System.currentTimeMillis();
            int duplicate = currEvent.eventID;
            FRJ1PartnerComputation(currEvent,historicalPartners,relatedEvents,kthValue,eventNum4partner);
            FRJ1RJProcess(eventNum4partner,duplicate,kthValue);
            step+=augment;
            end = System.currentTimeMillis();
            this.calPartner += (end-start);

            TAtimes++;
            eventNumInvolved++;
        }

        start = System.currentTimeMillis();
        int duplicate = -1;
        double kthValue = 0;
        if(topkEventPartnerQueue.size()>=param.queryTopK){
            kthValue = calculateTopKthValue();
        }
        while (step < eventNum4partner.size()){
            FRJ1RJProcess(eventNum4partner,duplicate,kthValue);
            step+=augment;
        }
        end = System.currentTimeMillis();
        this.calPartner += (end-start);

        for(HashMap.Entry<Integer,Pair> entry:mapTempEP.entrySet()){
            int currEvent = entry.getKey();
            Pair currPair = entry.getValue();
            double currValue = param.textualWeight * calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent)) + param.togetherWeight * currPair.score;
            topkEventPartnerQueue.add(new Pair(currEvent, currPair.partnerID, currValue));
        }

        round++;
        printTopkPairs("FRJ1.txt");
    }

    void FRJ1PartnerComputation(EventObject currEvent, HashSet<Integer> partnerSet, HashSet<Integer> relatedEvents,double kthValue,ArrayList<Pair> eventNum4partner){

        if(partnerSet.size() == 0 || relatedEvents.size() == 0){
            return;
        }

        ArrayList<Double> NSimilarityList = new ArrayList<>();

        HashMap<Integer,Double> eventsSimilarity = new HashMap<>();

        double NDenominator = 0;

        Iterator iterator = relatedEvents.iterator();
        while(iterator.hasNext()){
            int eventID = (Integer) iterator.next();
            double textualSimilarity = calculateTextualSimilarity(currEvent,eventsInfo.get(eventID));
            eventsSimilarity.put(eventID,textualSimilarity);
            NSimilarityList.add(textualSimilarity);
            NDenominator+=(textualSimilarity);
        }

        DoubleComparator doubleComparator = new DoubleComparator();
        NSimilarityList.sort(doubleComparator);

        ArrayList<Double>  NSimilaritySumList = new ArrayList<>();
        for (int i=0;i<NSimilarityList.size();i++){
            if(i==0){
                NSimilaritySumList.add(NSimilarityList.get(i));
            }
            else {
                NSimilaritySumList.add((NSimilaritySumList.get(i-1)+NSimilarityList.get(i)));
            }
        }

        double textual_similarity = calculateQueryTextualSimilarity(param.queryObject,currEvent);
        double pw = (kthValue-param.textualWeight*textual_similarity)/(1-param.textualWeight);
        int maxPartner = eventNum4partner.get(0).partnerID;
        int maxNum1NStar = Math.min(attendedEvents.size(),userToEvent.get(maxPartner).size());
        int maxNum1N = Math.min(relatedEvents.size(),userToEvent.get(maxPartner).size());
        double pb = NSimilaritySumList.get(maxNum1N-1);
        pb = (pb/NDenominator)*(maxNum1NStar/(double)attendedEvents.size());
        if(pw >= pb){
            eventNumPruning++;
            eventNumPruningByOpt1++;
            return;
        }

        double tempTopk = 0;
        if(mapTempEP.size()>=param.queryTopK){
            tempTopk = FRJTempTopKth();
        }
        if(tempTopk >= pb){
            eventNumPruning++;
            return;
        }

        double pv = 0;
        int finalPartner = -1;

        for(int i = 0; i < eventNum4partner.size() && i<step; i++){

            int currMaxPartner = eventNum4partner.get(i).partnerID;

            double currValue;
            double left;
            double right = 0;

            HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(attendedEvents);
            left = events4currMaxPartner.size();

            events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(relatedEvents);
            iterator = events4currMaxPartner.iterator();
            while(iterator.hasNext()){
                int eventID = (Integer) iterator.next();
                right+=eventsSimilarity.get(eventID);
            }

            currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
            if(currValue>pv){
                pv = currValue;
                finalPartner = currMaxPartner;
            }

            userNumInvolved++;
        }

        if(step>=eventNum4partner.size()){
            if (finalPartner != -1) {
                double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                topkEventPartnerQueue.add(new Pair(currEvent.eventID, finalPartner, FValue));
            }
        }
        else {
            mapTempEP.put(currEvent.eventID,new Pair(-1,finalPartner,pv));
            mapEventsSimilarity.put(currEvent.eventID, eventsSimilarity);
            mapNDenominator.put(currEvent.eventID, NDenominator);
            mapNSimilaritySumList.put(currEvent.eventID, NSimilaritySumList);
            mapRelatedEvents.put(currEvent.eventID, relatedEvents);
        }
    }

    void FRJ1RJProcess(ArrayList<Pair> eventNum4partner,int duplicate,double KthValue){

        ArrayList<Integer> removed = new ArrayList<>();

        double tempTopk = 0;
        if(mapTempEP.size()>=param.queryTopK){
            tempTopk = FRJTempTopKth();
        }

        for(HashMap.Entry<Integer,HashMap<Integer,Double>> entry:mapEventsSimilarity.entrySet()){

            int currEvent = entry.getKey();

            if(duplicate == currEvent){
                continue;
            }

            ArrayList<Double> NSimilaritySumList = mapNSimilaritySumList.get(currEvent);
            HashSet<Integer> relatedEvents = mapRelatedEvents.get(currEvent);
            Double NDenominator = mapNDenominator.get(currEvent);
            HashMap<Integer,Double> eventsSimilarity = mapEventsSimilarity.get(currEvent);

            double textualSimilarity = calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent));

            double pw = (KthValue - param.textualWeight * textualSimilarity) / (1 - param.textualWeight);
            int maxPartner = eventNum4partner.get(eventNum4partner.size()-1).partnerID;
            if((step-augment)<eventNum4partner.size()){
                maxPartner = eventNum4partner.get(step-augment).partnerID;
            }
            int maxNum1NStar = Math.min(attendedEvents.size(),userToEvent.get(maxPartner).size());
            int maxNum1N = Math.min(relatedEvents.size(),userToEvent.get(maxPartner).size());
            double pb = NSimilaritySumList.get(maxNum1N-1);
            pb = (pb/NDenominator)*(maxNum1NStar/(double)attendedEvents.size());
            if(pw >= pb){
                eventNumPruning++;
                Pair EP = mapTempEP.get(currEvent);
                if (EP.partnerID != -1) {
                    double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * EP.score;
                    topkEventPartnerQueue.add(new Pair(currEvent, EP.partnerID, FValue));
                }
                removed.add(currEvent);
                continue;
            }
            if(tempTopk >= pb){
                eventNumPruning++;
                Pair EP = mapTempEP.get(currEvent);
                if (EP.partnerID != -1) {
                    double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * EP.score;
                    topkEventPartnerQueue.add(new Pair(currEvent, EP.partnerID, FValue));
                }
                removed.add(currEvent);
                continue;
            }

            double pv = mapTempEP.get(currEvent).score;
            int finalPartner = mapTempEP.get(currEvent).partnerID;
            for(int i = step-augment; i < eventNum4partner.size() && i<step; i++){

                int currMaxPartner = eventNum4partner.get(i).partnerID;

                double currValue;
                double left;
                double right = 0;

                HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
                events4currMaxPartner.retainAll(attendedEvents);
                left = events4currMaxPartner.size();

                events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
                events4currMaxPartner.retainAll(relatedEvents);
                Iterator<Integer> iterator = events4currMaxPartner.iterator();
                while(iterator.hasNext()){
                    int eventID = iterator.next();
                    right+=eventsSimilarity.get(eventID);
                }

                currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
                if(currValue>pv){
                    pv = currValue;
                    finalPartner = currMaxPartner;
                }

                userNumInvolved++;
            }

            if(pv > mapTempEP.get(currEvent).score){
                mapTempEP.remove(currEvent);
                mapTempEP.put(currEvent,new Pair(-1,finalPartner,pv));
            }

            if(step>=eventNum4partner.size()){
                Pair EP = mapTempEP.get(currEvent);
                if (EP.partnerID != -1) {
                    double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * EP.score;
                    topkEventPartnerQueue.add(new Pair(currEvent, EP.partnerID, FValue));
                }
                removed.add(currEvent);
            }
        }

        for(int i = 0;i<removed.size();i++){
            mapEventsSimilarity.remove(removed.get(i));
            mapNDenominator.remove(removed.get(i));
            mapNSimilaritySumList.remove(removed.get(i));
            mapRelatedEvents.remove(removed.get(i));
            mapTempEP.remove(removed.get(i));
        }
    }

    void FRJ2(String query,int k,double alpha,int r) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);

        this.param.queryTopK = k;
        this.param.textualWeight = alpha;
        this.augment = r;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();

        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        long end = System.currentTimeMillis();
        this.calPre += (end-start);

        ArrayList<Pair> eventNum4partner = new ArrayList<>();
        iterator = historicalPartners.iterator();
        while (iterator.hasNext()) {
            int currPartner = (Integer) iterator.next();
            int currEventNum = userToEvent.get(currPartner).size();
            eventNum4partner.add(new Pair(-1,currPartner,currEventNum));
        }
        PairNumComparator customComparator = new PairNumComparator();
        eventNum4partner.sort(customComparator);

        while (!nextEventQueue.isEmpty()){
            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            start = System.currentTimeMillis();
            HashSet<Integer> relatedEvents = getFilteredRelatedEvents(currEvent,totalEvents);
            end = System.currentTimeMillis();
            this.calN += (end-start);

            start = System.currentTimeMillis();
            int duplicate = currEvent.eventID;
            FRJ2PartnerComputation(currEvent,historicalPartners,relatedEvents,kthValue,eventNum4partner);
            FRJ2RJProcess(eventNum4partner,duplicate,kthValue);
            step+=augment;
            end = System.currentTimeMillis();
            this.calPartner += (end-start);

            TAtimes++;
            eventNumInvolved++;
        }

        start = System.currentTimeMillis();
        int duplicate = -1;
        double kthValue = 0;
        if(topkEventPartnerQueue.size()>=param.queryTopK){
            kthValue = calculateTopKthValue();
        }
        while (step < eventNum4partner.size()){
            FRJ2RJProcess(eventNum4partner,duplicate,kthValue);
            step+=augment;
        }
        end = System.currentTimeMillis();
        this.calPartner += (end-start);

        for(HashMap.Entry<Integer,Pair> entry:mapTempEP.entrySet()){
            int currEvent = entry.getKey();
            Pair currPair = entry.getValue();
            double currValue = param.textualWeight * calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent)) + param.togetherWeight * currPair.score;
            topkEventPartnerQueue.add(new Pair(currEvent, currPair.partnerID, currValue));
        }

        round++;
        printTopkPairs("FRJ2.txt");
    }

    void FRJ2PartnerComputation(EventObject currEvent, HashSet<Integer> partnerSet, HashSet<Integer> relatedEvents,double kthValue,ArrayList<Pair> eventNum4partner){

        if(partnerSet.size() == 0 || relatedEvents.size() == 0){
            return;
        }

        ArrayList<Double> NSimilarityList = new ArrayList<>();

        HashMap<Integer,Double> eventsSimilarity = new HashMap<>();

        double NDenominator = 0;

        Iterator iterator = relatedEvents.iterator();
        while(iterator.hasNext()){
            int eventID = (Integer) iterator.next();
            double textualSimilarity = calculateTextualSimilarity(currEvent,eventsInfo.get(eventID));
            eventsSimilarity.put(eventID,textualSimilarity);
            NSimilarityList.add(textualSimilarity);
            NDenominator+=(textualSimilarity);
        }

        DoubleComparator doubleComparator = new DoubleComparator();
        NSimilarityList.sort(doubleComparator);

        ArrayList<Double>  NSimilaritySumList = new ArrayList<>();
        for (int i=0;i<NSimilarityList.size();i++){
            if(i==0){
                NSimilaritySumList.add(NSimilarityList.get(i));
            }
            else {
                NSimilaritySumList.add((NSimilaritySumList.get(i-1)+NSimilarityList.get(i)));
            }
        }

        double textual_similarity = calculateQueryTextualSimilarity(param.queryObject,currEvent);

        long start = System.currentTimeMillis();
        double tempTopk = 0;
        if(mapTempEP.size()>=param.queryTopK){
            tempTopk = FRJTempTopKth();
        }
        long end = System.currentTimeMillis();
        this.cal3 += (end-start);

        double pv = 0;
        int finalPartner = -1;

        for(int i = 0; i < eventNum4partner.size() && i<step; i++){

            int currMaxPartner = eventNum4partner.get(i).partnerID;
            int curr1 = Math.min(relatedEvents.size(),userToEvent.get(currMaxPartner).size());
            int curr2 = Math.min(attendedEvents.size(),userToEvent.get(currMaxPartner).size());
            double tmpMax = (NSimilaritySumList.get(curr1-1)/NDenominator)*(curr2/(double)attendedEvents.size());
            if(pv >= tmpMax){
                eventNumPruning++;
                if(finalPartner != -1){
                    double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                    topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
                }
                return;
            }
            if(tempTopk >= (param.textualWeight * textual_similarity + param.togetherWeight * tmpMax)){
                eventNumPruning++;
                if (finalPartner != -1) {
                    double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                    topkEventPartnerQueue.add(new Pair(currEvent.eventID, finalPartner, FValue));
                }
                return;
            }

            double currValue;
            double left;
            double right = 0;

            HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(attendedEvents);
            left = events4currMaxPartner.size();

            events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(relatedEvents);
            iterator = events4currMaxPartner.iterator();
            while(iterator.hasNext()){
                int eventID = (Integer) iterator.next();
                right+=eventsSimilarity.get(eventID);
            }

            currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
            if(currValue>pv){
                pv = currValue;
                finalPartner = currMaxPartner;
            }

            userNumInvolved++;
        }

        if(step>=eventNum4partner.size()){
            if (finalPartner != -1) {
                double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                topkEventPartnerQueue.add(new Pair(currEvent.eventID, finalPartner, FValue));
            }
        }
        else {
            mapTempEP.put(currEvent.eventID,new Pair(-1,finalPartner,pv));
            mapEventsSimilarity.put(currEvent.eventID, eventsSimilarity);
            mapNDenominator.put(currEvent.eventID, NDenominator);
            mapNSimilaritySumList.put(currEvent.eventID, NSimilaritySumList);
            mapRelatedEvents.put(currEvent.eventID, relatedEvents);
        }
    }

    void FRJ2RJProcess(ArrayList<Pair> eventNum4partner,int duplicate,double KthValue){

        ArrayList<Integer> removed = new ArrayList<>();

        long start = System.currentTimeMillis();
        double tempTopk = 0;
        if(mapTempEP.size()>=param.queryTopK){
            tempTopk = FRJTempTopKth();
        }
        long end = System.currentTimeMillis();
        this.cal3 += (end-start);

        for(HashMap.Entry<Integer,HashMap<Integer,Double>> entry:mapEventsSimilarity.entrySet()){

            int currEvent = entry.getKey();

            if(duplicate == currEvent){
                continue;
            }

            boolean jump = false;

            ArrayList<Double> NSimilaritySumList = mapNSimilaritySumList.get(currEvent);
            HashSet<Integer> relatedEvents = mapRelatedEvents.get(currEvent);
            Double NDenominator = mapNDenominator.get(currEvent);
            HashMap<Integer,Double> eventsSimilarity = mapEventsSimilarity.get(currEvent);

            double textualSimilarity = calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent));

            double pv = mapTempEP.get(currEvent).score;
            int finalPartner = mapTempEP.get(currEvent).partnerID;
            for(int i = step-augment; i < eventNum4partner.size() && i<step; i++){

                if(jump){
                    break;
                }

                int currMaxPartner = eventNum4partner.get(i).partnerID;
                int curr1 = Math.min(relatedEvents.size(),userToEvent.get(currMaxPartner).size());
                int curr2 = Math.min(attendedEvents.size(),userToEvent.get(currMaxPartner).size());

                double tmpMax = (NSimilaritySumList.get(curr1-1)/NDenominator)*(curr2/(double)attendedEvents.size());
                if(pv >= tmpMax){
                    eventNumPruning++;
                    if(finalPartner != -1){
                        double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent,finalPartner,FValue));
                    }
                    removed.add(currEvent);
                    jump = true;
                    continue;
                }
                if(tempTopk >= (param.textualWeight * textualSimilarity + param.togetherWeight * tmpMax)){
                    eventNumPruning++;
                    if (finalPartner != -1) {
                        double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent, finalPartner, FValue));
                    }
                    removed.add(currEvent);
                    jump = true;
                    continue;
                }

                double currValue;
                double left;
                double right = 0;

                HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
                events4currMaxPartner.retainAll(attendedEvents);
                left = events4currMaxPartner.size();

                events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
                events4currMaxPartner.retainAll(relatedEvents);
                Iterator<Integer> iterator = events4currMaxPartner.iterator();
                while(iterator.hasNext()){
                    int eventID = (Integer) iterator.next();
                    right+=eventsSimilarity.get(eventID);
                }

                currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
                if(currValue>pv){
                    pv = currValue;
                    finalPartner = currMaxPartner;
                }

                userNumInvolved++;
            }

            if(jump){
                continue;
            }

            if(pv > mapTempEP.get(currEvent).score){
                mapTempEP.remove(currEvent);
                mapTempEP.put(currEvent,new Pair(-1,finalPartner,pv));
            }

            if(step>=eventNum4partner.size()){
                Pair EP = mapTempEP.get(currEvent);
                if (EP.partnerID != -1) {
                    double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * EP.score;
                    topkEventPartnerQueue.add(new Pair(currEvent, EP.partnerID, FValue));
                }
                removed.add(currEvent);
            }
        }

        for(int i = 0;i<removed.size();i++){
            mapEventsSimilarity.remove(removed.get(i));
            mapNDenominator.remove(removed.get(i));
            mapNSimilaritySumList.remove(removed.get(i));
            mapRelatedEvents.remove(removed.get(i));
            mapTempEP.remove(removed.get(i));
        }
    }

    void FRJ3(String query,int k,double alpha,int r) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);

        this.param.queryTopK = k;
        this.param.textualWeight = alpha;
        this.augment = r;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();

        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        long end = System.currentTimeMillis();
        this.calPre += (end-start);

        start = System.currentTimeMillis();
        HashMap<Integer,Integer> auxiliaryU2R = new HashMap<>();
        HashMap<Integer,HashSet<Integer>> auxiliaryR2Us = new HashMap<>();
        ArrayList<Integer> ROrder = new ArrayList<>();
        if(userToEvent.containsKey(param.queryUserID)){
            iterator = historicalPartners.iterator();
            while (iterator.hasNext()){
                int currUser = iterator.next();
                if(userToEvent.containsKey(currUser)){
                    HashSet<Integer> tmp = new HashSet<>(userToEvent.get(param.queryUserID));
                    tmp.retainAll(userToEvent.get(currUser));
                    int R = tmp.size();
                    auxiliaryU2R.put(currUser,R);
                    if(!auxiliaryR2Us.containsKey(R)){
                        auxiliaryR2Us.put(R,new HashSet<>());
                        ROrder.add(R);
                    }
                    auxiliaryR2Us.get(R).add(currUser);
                }
            }
        }
        IntegerComparator ic = new IntegerComparator();
        ROrder.sort(ic);
        end = System.currentTimeMillis();
        this.cal1 += (end-start);

        start = System.currentTimeMillis();
        HashMap<Integer,Integer> auxiliaryU2E = new HashMap<>();
        HashMap<Integer,HashSet<Integer>> auxiliaryE2Us = new HashMap<>();
        ArrayList<Integer> EOrder = new ArrayList<>();
        iterator = historicalPartners.iterator();
        while (iterator.hasNext()){
            int currUser = iterator.next();
            if(userToEvent.containsKey(currUser)){
                int E = userToEvent.get(currUser).size();
                auxiliaryU2E.put(currUser,E);
                if(!auxiliaryE2Us.containsKey(E)){
                    auxiliaryE2Us.put(E,new HashSet<>());
                    EOrder.add(E);
                }
                auxiliaryE2Us.get(E).add(currUser);
            }
        }
        EOrder.sort(ic);

        ArrayList<Pair> eventNum4partner = new ArrayList<>();
        for(HashMap.Entry<Integer, Integer> entry: auxiliaryU2E.entrySet())
        {
            int currUser = entry.getKey();
            int currE = auxiliaryU2E.get(currUser);
            int currR = auxiliaryU2R.get(currUser);
            eventNum4partner.add(new Pair(-1,currUser,currE*currR));
        }
        PairNumComparator customComparator = new PairNumComparator();
        eventNum4partner.sort(customComparator);
        end = System.currentTimeMillis();
        this.cal2 += (end-start);

        while (!nextEventQueue.isEmpty()){
            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            start = System.currentTimeMillis();
            HashSet<Integer> relatedEvents = getFilteredRelatedEvents(currEvent,totalEvents);
            end = System.currentTimeMillis();
            this.calN += (end-start);

            start = System.currentTimeMillis();
            int duplicate = currEvent.eventID;
            FRJ3PartnerComputation(currEvent,historicalPartners,relatedEvents,kthValue,ROrder,auxiliaryU2R,auxiliaryR2Us,EOrder,auxiliaryU2E,auxiliaryE2Us,eventNum4partner);
            FRJ3RJProcess(ROrder,auxiliaryU2R,auxiliaryR2Us,EOrder,auxiliaryU2E,auxiliaryE2Us,eventNum4partner,duplicate,kthValue);
            step+=augment;
            end = System.currentTimeMillis();
            this.calPartner += (end-start);

            TAtimes++;
            eventNumInvolved++;
        }

        start = System.currentTimeMillis();
        int duplicate = -1;
        double kthValue = 0;
        if(topkEventPartnerQueue.size()>=param.queryTopK){
            kthValue = calculateTopKthValue();
        }
        while (step < eventNum4partner.size()){
            FRJ3RJProcess(ROrder,auxiliaryU2R,auxiliaryR2Us,EOrder,auxiliaryU2E,auxiliaryE2Us,eventNum4partner,duplicate,kthValue);
            step+=augment;
        }
        end = System.currentTimeMillis();
        this.calPartner += (end-start);

        for(HashMap.Entry<Integer,Pair> entry:mapTempEP.entrySet()){
            int currEvent = entry.getKey();
            Pair currPair = entry.getValue();
            double currValue = param.textualWeight * calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent)) + param.togetherWeight * currPair.score;
            topkEventPartnerQueue.add(new Pair(currEvent, currPair.partnerID, currValue));
        }

        round++;
        printTopkPairs("FRJ3.txt");
    }

    void FRJ3PartnerComputation(EventObject currEvent, HashSet<Integer> partnerSet, HashSet<Integer> relatedEvents,double kthValue,ArrayList<Integer> ROrder,HashMap<Integer,Integer> auxiliaryU2R,HashMap<Integer,HashSet<Integer>> auxiliaryR2Us,ArrayList<Integer> EOrder,HashMap<Integer,Integer> auxiliaryU2E,HashMap<Integer,HashSet<Integer>> auxiliaryE2Us,ArrayList<Pair> eventNum4partner){

        if(partnerSet.size() == 0 || relatedEvents.size() == 0){
            return;
        }

        ArrayList<Double> NSimilarityList = new ArrayList<>();

        HashMap<Integer,Double> eventsSimilarity = new HashMap<>();

        double NDenominator = 0;

        Iterator iterator = relatedEvents.iterator();
        while(iterator.hasNext()){
            int eventID = (Integer) iterator.next();
            double textualSimilarity = calculateTextualSimilarity(currEvent,eventsInfo.get(eventID));
            eventsSimilarity.put(eventID,textualSimilarity);
            NSimilarityList.add(textualSimilarity);
            NDenominator+=(textualSimilarity);
        }

        DoubleComparator doubleComparator = new DoubleComparator();
        NSimilarityList.sort(doubleComparator);

        ArrayList<Double>  NSimilaritySumList = new ArrayList<>();
        for (int i=0;i<NSimilarityList.size();i++){
            if(i==0){
                NSimilaritySumList.add(NSimilarityList.get(i));
            }
            else {
                NSimilaritySumList.add((NSimilaritySumList.get(i-1)+NSimilarityList.get(i)));
            }
        }

        double textual_similarity = calculateQueryTextualSimilarity(param.queryObject,currEvent);

        long start = System.currentTimeMillis();
        double tempTopk = 0;
        if(mapTempEP.size()>=param.queryTopK){
            tempTopk = FRJTempTopKth();
        }
        long end = System.currentTimeMillis();
        this.cal3 += (end-start);

        REIndex REindex = new REIndex();
        HashMap<Integer,Integer> RCount = new HashMap<>();
        HashMap<Integer,Integer> ECount = new HashMap<>();

        double pv = 0;
        int finalPartner = -1;

        for(int i = 0; i < eventNum4partner.size() && i<step; i++){

            int curr1 = 0;
            if(EOrder.size() > REindex.k2){
                int currE = EOrder.get(REindex.k2);
                curr1 = Math.min(relatedEvents.size(),currE);
                while (ECount.containsKey(currE) && auxiliaryE2Us.get(currE).size() == ECount.get(currE)){
                    REindex.k2++;
                    if(EOrder.size() <= REindex.k2){
                        curr1 = 0;
                        break;
                    }
                    currE = EOrder.get(REindex.k2);
                    curr1 = Math.min(relatedEvents.size(),currE);
                }
            }

            int curr2 = 0;
            if(ROrder.size() > REindex.k1){
                int currR = ROrder.get(REindex.k1);
                curr2 = currR;
                while (RCount.containsKey(currR) && auxiliaryR2Us.get(currR).size() == RCount.get(currR)){
                    REindex.k1++;
                    if(ROrder.size() <= REindex.k1){
                        curr2 = 0;
                        break;
                    }
                    currR = ROrder.get(REindex.k1);
                    curr2 = currR;
                }
            }

            double tmpMax = (NSimilaritySumList.get(curr1-1)/NDenominator)*(curr2/(double)attendedEvents.size());
            if(pv >= tmpMax){
                eventNumPruning++;
                if(finalPartner != -1){
                    double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                    topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
                }
                return;
            }
            if(tempTopk >= (param.textualWeight * textual_similarity + param.togetherWeight * tmpMax)){
                eventNumPruning++;
                if (finalPartner != -1) {
                    double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                    topkEventPartnerQueue.add(new Pair(currEvent.eventID, finalPartner, FValue));
                }
                return;
            }

            double currValue;
            double left;
            double right = 0;

            int currMaxPartner = eventNum4partner.get(i).partnerID;
            HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(attendedEvents);
            left = events4currMaxPartner.size();

            events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(relatedEvents);
            iterator = events4currMaxPartner.iterator();
            while(iterator.hasNext()){
                int eventID = (Integer) iterator.next();
                right+=eventsSimilarity.get(eventID);
            }

            currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
            if(currValue>pv){
                pv = currValue;
                finalPartner = currMaxPartner;
            }

            if(auxiliaryU2R.containsKey(currMaxPartner)){
                int currR = auxiliaryU2R.get(currMaxPartner);
                if(!RCount.containsKey(currR)){
                    RCount.put(currR,1);
                }
                else {
                    int tmp = RCount.get(currR);
                    RCount.remove(currR);
                    RCount.put(currR,tmp+1);
                }
            }
            if(auxiliaryU2E.containsKey(currMaxPartner)){
                int currE = auxiliaryU2E.get(currMaxPartner);
                if(!ECount.containsKey(currE)){
                    ECount.put(currE,1);
                }
                else {
                    int tmp = ECount.get(currE);
                    ECount.remove(currE);
                    ECount.put(currE,tmp+1);
                }
            }
            userNumInvolved++;
        }

        if(step>=eventNum4partner.size()){
            if (finalPartner != -1) {
                double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                topkEventPartnerQueue.add(new Pair(currEvent.eventID, finalPartner, FValue));
            }
        }
        else {
            mapTempEP.put(currEvent.eventID,new Pair(-1,finalPartner,pv));
            mapEventsSimilarity.put(currEvent.eventID, eventsSimilarity);
            mapNDenominator.put(currEvent.eventID, NDenominator);
            mapNSimilaritySumList.put(currEvent.eventID, NSimilaritySumList);
            mapRelatedEvents.put(currEvent.eventID, relatedEvents);
            mapRCount.put(currEvent.eventID,RCount);
            mapECount.put(currEvent.eventID,ECount);
            mapREIndex.put(currEvent.eventID,REindex);
        }
    }

    void FRJ3RJProcess(ArrayList<Integer> ROrder,HashMap<Integer,Integer> auxiliaryU2R,HashMap<Integer,HashSet<Integer>> auxiliaryR2Us,ArrayList<Integer> EOrder,HashMap<Integer,Integer> auxiliaryU2E,HashMap<Integer,HashSet<Integer>> auxiliaryE2Us,ArrayList<Pair> eventNum4partner,int duplicate,double KthValue){

        ArrayList<Integer> removed = new ArrayList<>();

        long start = System.currentTimeMillis();
        double tempTopk = 0;
        if(mapTempEP.size()>=param.queryTopK){
            tempTopk = FRJTempTopKth();
        }
        long end = System.currentTimeMillis();
        this.cal3 += (end-start);

        for(HashMap.Entry<Integer,HashMap<Integer,Double>> entry:mapEventsSimilarity.entrySet()){

            int currEvent = entry.getKey();

            if(duplicate == currEvent){
                continue;
            }

            boolean jump = false;

            ArrayList<Double> NSimilaritySumList = mapNSimilaritySumList.get(currEvent);
            HashSet<Integer> relatedEvents = mapRelatedEvents.get(currEvent);
            Double NDenominator = mapNDenominator.get(currEvent);
            HashMap<Integer,Double> eventsSimilarity = mapEventsSimilarity.get(currEvent);
            REIndex REindex = mapREIndex.get(currEvent);
            HashMap<Integer,Integer> RCount = mapRCount.get(currEvent);
            HashMap<Integer,Integer> ECount = mapECount.get(currEvent);

            double textualSimilarity = calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent));

            double pv = mapTempEP.get(currEvent).score;
            int finalPartner = mapTempEP.get(currEvent).partnerID;
            for(int i = step-augment; i < eventNum4partner.size() && i<step; i++){

                if(jump){
                    break;
                }

                int curr1 = 0;
                if(EOrder.size() > REindex.k2){
                    int currE = EOrder.get(REindex.k2);
                    curr1 = Math.min(relatedEvents.size(),currE);
                    while (ECount.containsKey(currE) && auxiliaryE2Us.get(currE).size() == ECount.get(currE)){
                        REindex.k2++;
                        if(EOrder.size() <= REindex.k2){
                            curr1 = 0;
                            break;
                        }
                        currE = EOrder.get(REindex.k2);
                        curr1 = Math.min(relatedEvents.size(),currE);
                    }
                }

                int curr2 = 0;
                if(ROrder.size() > REindex.k1){
                    int currR = ROrder.get(REindex.k1);
                    curr2 = currR;
                    while (RCount.containsKey(currR) && auxiliaryR2Us.get(currR).size() == RCount.get(currR)){
                        REindex.k1++;
                        if(ROrder.size() <= REindex.k1){
                            curr2 = 0;
                            break;
                        }
                        currR = ROrder.get(REindex.k1);
                        curr2 = currR;
                    }
                }

                double tmpMax = (NSimilaritySumList.get(curr1-1)/NDenominator)*(curr2/(double)attendedEvents.size());
                if(pv >= tmpMax){
                    eventNumPruning++;
                    if(finalPartner != -1){
                        double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent,finalPartner,FValue));
                    }
                    removed.add(currEvent);
                    jump = true;
                    continue;
                }
                if(tempTopk >= (param.textualWeight * textualSimilarity + param.togetherWeight * tmpMax)){
                    eventNumPruning++;
                    if (finalPartner != -1) {
                        double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent, finalPartner, FValue));
                    }
                    removed.add(currEvent);
                    jump = true;
                    continue;
                }

                double currValue;
                double left;
                double right = 0;

                int currMaxPartner = eventNum4partner.get(i).partnerID;
                HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
                events4currMaxPartner.retainAll(attendedEvents);
                left = events4currMaxPartner.size();

                events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
                events4currMaxPartner.retainAll(relatedEvents);
                Iterator<Integer> iterator = events4currMaxPartner.iterator();
                while(iterator.hasNext()){
                    int eventID = (Integer) iterator.next();
                    right+=eventsSimilarity.get(eventID);
                }

                currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
                if(currValue>pv){
                    pv = currValue;
                    finalPartner = currMaxPartner;
                }

                if(auxiliaryU2R.containsKey(currMaxPartner)){
                    int currR = auxiliaryU2R.get(currMaxPartner);
                    if(!RCount.containsKey(currR)){
                        RCount.put(currR,1);
                    }
                    else {
                        int tmp = RCount.get(currR);
                        RCount.remove(currR);
                        RCount.put(currR,tmp+1);
                    }
                }
                if(auxiliaryU2E.containsKey(currMaxPartner)){
                    int currE = auxiliaryU2E.get(currMaxPartner);
                    if(!ECount.containsKey(currE)){
                        ECount.put(currE,1);
                    }
                    else {
                        int tmp = ECount.get(currE);
                        ECount.remove(currE);
                        ECount.put(currE,tmp+1);
                    }
                }
                userNumInvolved++;
            }

            if(jump){
                continue;
            }

            if(pv > mapTempEP.get(currEvent).score){
                mapTempEP.remove(currEvent);
                mapTempEP.put(currEvent,new Pair(-1,finalPartner,pv));
            }

            if(step>=eventNum4partner.size()){
                Pair EP = mapTempEP.get(currEvent);
                if (EP.partnerID != -1) {
                    double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * EP.score;
                    topkEventPartnerQueue.add(new Pair(currEvent, EP.partnerID, FValue));
                }
                removed.add(currEvent);
            }
        }

        for(int i = 0;i<removed.size();i++){
            mapEventsSimilarity.remove(removed.get(i));
            mapNDenominator.remove(removed.get(i));
            mapNSimilaritySumList.remove(removed.get(i));
            mapRelatedEvents.remove(removed.get(i));
            mapTempEP.remove(removed.get(i));
            mapRCount.remove(removed.get(i));
            mapECount.remove(removed.get(i));
            mapREIndex.remove(removed.get(i));
        }
    }

    void FRJStar(String query,int k,double alpha,int r) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);

        this.param.queryTopK = k;
        this.param.textualWeight = alpha;
        this.augment = r;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();

        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        HashMap<Integer,HashSet<Integer>> historicalPartnerInRelatedEvents = new HashMap<>();
        iterator = totalEvents.iterator();
        while(iterator.hasNext()){
            int currE = iterator.next();
            HashSet<Integer> currU = eventToUsers.get(currE);
            Iterator<Integer> it = currU.iterator();
            HashSet<Integer> historicalPartnerInSingleRelatedEvents = new HashSet<>();
            while (it.hasNext()){
                int user = it.next();
                if(historicalPartners.contains(user)){
                    historicalPartnerInSingleRelatedEvents.add(user);
                }
            }
            historicalPartnerInRelatedEvents.put(currE,historicalPartnerInSingleRelatedEvents);
        }
        long end = System.currentTimeMillis();
        this.calPre += (end-start);

        start = System.currentTimeMillis();
        HashMap<Integer,Integer> auxiliaryU2R = new HashMap<>();
        HashMap<Integer,HashSet<Integer>> auxiliaryR2Us = new HashMap<>();
        ArrayList<Integer> ROrder = new ArrayList<>();
        if(userToEvent.containsKey(param.queryUserID)){
            iterator = historicalPartners.iterator();
            while (iterator.hasNext()){
                int currUser = iterator.next();
                if(userToEvent.containsKey(currUser)){
                    HashSet<Integer> tmp = new HashSet<>(userToEvent.get(param.queryUserID));
                    tmp.retainAll(userToEvent.get(currUser));
                    int R = tmp.size();
                    auxiliaryU2R.put(currUser,R);
                    if(!auxiliaryR2Us.containsKey(R)){
                        auxiliaryR2Us.put(R,new HashSet<>());
                        ROrder.add(R);
                    }
                    auxiliaryR2Us.get(R).add(currUser);
                }
            }
        }
        IntegerComparator ic = new IntegerComparator();
        ROrder.sort(ic);
        end = System.currentTimeMillis();
        this.cal1 += (end-start);

        start = System.currentTimeMillis();
        HashMap<Integer,Integer> auxiliaryU2E = new HashMap<>();
        HashMap<Integer,HashSet<Integer>> auxiliaryE2Us = new HashMap<>();
        ArrayList<Integer> EOrder = new ArrayList<>();
        iterator = historicalPartners.iterator();
        while (iterator.hasNext()){
            int currUser = iterator.next();
            if(userToEvent.containsKey(currUser)){
                int E = userToEvent.get(currUser).size();
                auxiliaryU2E.put(currUser,E);
                if(!auxiliaryE2Us.containsKey(E)){
                    auxiliaryE2Us.put(E,new HashSet<>());
                    EOrder.add(E);
                }
                auxiliaryE2Us.get(E).add(currUser);
            }
        }
        EOrder.sort(ic);

        ArrayList<Pair> eventNum4partner = new ArrayList<>();
        for(HashMap.Entry<Integer, Integer> entry: auxiliaryU2E.entrySet())
        {
            int currUser = entry.getKey();
            int currE = auxiliaryU2E.get(currUser);
            int currR = auxiliaryU2R.get(currUser);
            eventNum4partner.add(new Pair(-1,currUser,currE*currR));
        }
        PairNumComparator customComparator = new PairNumComparator();
        eventNum4partner.sort(customComparator);
        end = System.currentTimeMillis();
        this.cal2 += (end-start);

        while (!nextEventQueue.isEmpty()){
            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            start = System.currentTimeMillis();
            HashSet<Integer> relatedEvents = new HashSet<>();
            HashSet<Integer> presentU = new HashSet<>();
            ArrayList<Integer> candidateEvents;
            for (HashMap.Entry<String, Integer> entry : currEvent.wordsCount.entrySet()) {
                String currWord = entry.getKey();
                candidateEvents = InvertedIndex.get(currWord);
                for(int i=0;i<candidateEvents.size();i++){
                    int curr = candidateEvents.get(i);
                    if(totalEvents.contains(curr) && isNeighborhood(currEvent,eventsInfo.get(curr))){
                        relatedEvents.add(curr);
                        if(historicalPartnerInRelatedEvents.containsKey(curr)){
                            presentU.addAll(historicalPartnerInRelatedEvents.get(curr));
                        }
                    }
                }
            }
            end = System.currentTimeMillis();
            this.calN += (end-start);

            start = System.currentTimeMillis();
            HashMap<Integer,Integer> RCount = new HashMap<>();
            HashMap<Integer,Integer> ECount = new HashMap<>();
            iterator = presentU.iterator();
            while (iterator.hasNext()) {
                int currU = iterator.next();
                if(auxiliaryU2R.containsKey(currU)){
                    int currR = auxiliaryU2R.get(currU);
                    if(!RCount.containsKey(currR)){
                        RCount.put(currR,1);
                    }
                    else {
                        int tmp = RCount.get(currR);
                        RCount.remove(currR);
                        RCount.put(currR,tmp+1);
                    }
                }
                if(auxiliaryU2E.containsKey(currU)){
                    int currE = auxiliaryU2E.get(currU);
                    if(!ECount.containsKey(currE)){
                        ECount.put(currE,1);
                    }
                    else {
                        int tmp = ECount.get(currE);
                        ECount.remove(currE);
                        ECount.put(currE,tmp+1);
                    }
                }
            }
            end = System.currentTimeMillis();
            this.calU += (end-start);

            start = System.currentTimeMillis();
            int duplicate = currEvent.eventID;
            FRJStarPartnerComputation(currEvent,historicalPartners,relatedEvents,kthValue,ROrder,auxiliaryU2R,auxiliaryR2Us,EOrder,auxiliaryU2E,auxiliaryE2Us,eventNum4partner,RCount,ECount,presentU);
            FRJStarRJProcess(ROrder,auxiliaryU2R,auxiliaryR2Us,EOrder,auxiliaryU2E,auxiliaryE2Us,eventNum4partner,duplicate,kthValue);
            step+=augment;
            end = System.currentTimeMillis();
            this.calPartner += (end-start);

            TAtimes++;
            eventNumInvolved++;
        }

        start = System.currentTimeMillis();
        int duplicate = -1;
        double kthValue = 0;
        if(topkEventPartnerQueue.size()>=param.queryTopK){
            kthValue = calculateTopKthValue();
        }
        while (step < eventNum4partner.size()){
            FRJStarRJProcess(ROrder,auxiliaryU2R,auxiliaryR2Us,EOrder,auxiliaryU2E,auxiliaryE2Us,eventNum4partner,duplicate,kthValue);
            step+=augment;
        }
        end = System.currentTimeMillis();
        this.calPartner += (end-start);

        for(HashMap.Entry<Integer,Pair> entry:mapTempEP.entrySet()){
            int currEvent = entry.getKey();
            Pair currPair = entry.getValue();
            double currValue = param.textualWeight * calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent)) + param.togetherWeight * currPair.score;
            topkEventPartnerQueue.add(new Pair(currEvent, currPair.partnerID, currValue));
        }

        round++;
        printTopkPairs("FRJStar.txt");
    }

    void FRJStarPartnerComputation(EventObject currEvent, HashSet<Integer> partnerSet, HashSet<Integer> relatedEvents,double kthValue,ArrayList<Integer> ROrder,HashMap<Integer,Integer> auxiliaryU2R,HashMap<Integer,HashSet<Integer>> auxiliaryR2Us,ArrayList<Integer> EOrder,HashMap<Integer,Integer> auxiliaryU2E,HashMap<Integer,HashSet<Integer>> auxiliaryE2Us,ArrayList<Pair> eventNum4partner,HashMap<Integer,Integer> RCount,HashMap<Integer,Integer> ECount,HashSet<Integer> presentU){

        if(partnerSet.size() == 0 || relatedEvents.size() == 0){
            return;
        }

        ArrayList<Double> NSimilarityList = new ArrayList<>();

        HashMap<Integer,Double> eventsSimilarity = new HashMap<>();

        double NDenominator = 0;

        Iterator iterator = relatedEvents.iterator();
        while(iterator.hasNext()){
            int eventID = (Integer) iterator.next();
            double textualSimilarity = calculateTextualSimilarity(currEvent,eventsInfo.get(eventID));
            eventsSimilarity.put(eventID,textualSimilarity);
            NSimilarityList.add(textualSimilarity);
            NDenominator+=(textualSimilarity);
        }

        DoubleComparator doubleComparator = new DoubleComparator();
        NSimilarityList.sort(doubleComparator);

        ArrayList<Double>  NSimilaritySumList = new ArrayList<>();
        for (int i=0;i<NSimilarityList.size();i++){
            if(i==0){
                NSimilaritySumList.add(NSimilarityList.get(i));
            }
            else {
                NSimilaritySumList.add((NSimilaritySumList.get(i-1)+NSimilarityList.get(i)));
            }
        }

        double textual_similarity = calculateQueryTextualSimilarity(param.queryObject,currEvent);
        double pw = (kthValue-param.textualWeight*textual_similarity)/(1-param.textualWeight);
        int maxNum1NStar = 0;
        int maxNum1N = 0;
        REIndex REindex = new REIndex();
        int index = 0;
        while (ROrder.size()>index && !RCount.containsKey(ROrder.get(index))){
            index++;
        }
        if(ROrder.size()>index){
            maxNum1NStar = ROrder.get(index);
        }
        REindex.k1 = index;
        index = 0;
        while (EOrder.size()>index && !ECount.containsKey(EOrder.get(index))){
            index++;
        }
        if(EOrder.size()>index){
            maxNum1N = Math.min(relatedEvents.size(),EOrder.get(index));
        }
        REindex.k2 = index;
        double pb = 0;
        if(maxNum1N != 0){
            pb = NSimilaritySumList.get(maxNum1N-1);
        }
        pb = (pb/NDenominator)*(maxNum1NStar/(double)attendedEvents.size());
        if(pw >= pb){
            eventNumPruning++;
            eventNumPruningByOpt1++;
            return;
        }

        long start = System.currentTimeMillis();
        double tempTopk = 0;
        if(mapTempEP.size()>=param.queryTopK){
            tempTopk = FRJTempTopKth();
        }
        long end = System.currentTimeMillis();
        this.cal3 += (end-start);

        double pv = 0;
        int finalPartner = -1;

        int scanned = 0;

        for(int i = 0; i < eventNum4partner.size() && i<step; i++){

            if(scanned == presentU.size()){
                eventNumPruning++;
                if (finalPartner != -1) {
                    double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                    topkEventPartnerQueue.add(new Pair(currEvent.eventID, finalPartner, FValue));
                }
                return;
            }

            int currMaxPartner = eventNum4partner.get(i).partnerID;
            if(!presentU.contains(currMaxPartner)){
                continue;
            }

            int curr1 = 0;
            if(EOrder.size() > REindex.k2){
                int currE = EOrder.get(REindex.k2);
                curr1 = Math.min(relatedEvents.size(),currE);
                while (!ECount.containsKey(currE) || ECount.get(currE) == 0){
                    REindex.k2++;
                    if(EOrder.size() <= REindex.k2){
                        curr1 = 0;
                        break;
                    }
                    currE = EOrder.get(REindex.k2);
                    curr1 = Math.min(relatedEvents.size(),currE);
                }
            }

            int curr2 = 0;
            if(ROrder.size() > REindex.k1){
                int currR = ROrder.get(REindex.k1);
                curr2 = currR;
                while (!RCount.containsKey(currR) || RCount.get(currR) == 0){
                    REindex.k1++;
                    if(ROrder.size() <= REindex.k1){
                        curr2 = 0;
                        break;
                    }
                    currR = ROrder.get(REindex.k1);
                    curr2 = currR;
                }
            }

            double tmpMax = 0;
            if(curr1 != 0 && curr2 != 0){
                tmpMax = (NSimilaritySumList.get(curr1-1)/NDenominator)*(curr2/(double)attendedEvents.size());
            }
            if (pw >= tmpMax) {
                eventNumPruning++;
                if (finalPartner != -1) {
                    double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                    topkEventPartnerQueue.add(new Pair(currEvent.eventID, finalPartner, FValue));
                }
                return;
            }
            if(pv >= tmpMax){
                eventNumPruning++;
                if(finalPartner != -1){
                    double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                    topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
                }
                return;
            }
            if(tempTopk >= (param.textualWeight * textual_similarity + param.togetherWeight * tmpMax)){
                eventNumPruning++;
                if (finalPartner != -1) {
                    double FValue = param.textualWeight * textual_similarity + param.togetherWeight * pv;
                    topkEventPartnerQueue.add(new Pair(currEvent.eventID, finalPartner, FValue));
                }
                return;
            }

            double currValue;
            double left;
            double right = 0;

            HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(attendedEvents);
            left = events4currMaxPartner.size();

            events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
            events4currMaxPartner.retainAll(relatedEvents);
            iterator = events4currMaxPartner.iterator();
            while(iterator.hasNext()){
                int eventID = (Integer) iterator.next();
                right+=eventsSimilarity.get(eventID);
            }

            currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
            if(currValue>pv){
                pv = currValue;
                finalPartner = currMaxPartner;
            }

            if(auxiliaryU2R.containsKey(currMaxPartner)){
                int currR = auxiliaryU2R.get(currMaxPartner);
                int tmp = RCount.get(currR);
                RCount.remove(currR);
                RCount.put(currR,tmp-1);
            }
            if(auxiliaryU2E.containsKey(currMaxPartner)){
                int currE = auxiliaryU2E.get(currMaxPartner);
                int tmp = ECount.get(currE);
                ECount.remove(currE);
                ECount.put(currE,tmp-1);
            }

            userNumInvolved++;
            scanned++;
        }

        mapTempEP.put(currEvent.eventID,new Pair(-1,finalPartner,pv));
        mapEventsSimilarity.put(currEvent.eventID, eventsSimilarity);
        mapNDenominator.put(currEvent.eventID, NDenominator);
        mapNSimilaritySumList.put(currEvent.eventID, NSimilaritySumList);
        mapRelatedEvents.put(currEvent.eventID, relatedEvents);
        mapRCount.put(currEvent.eventID,RCount);
        mapECount.put(currEvent.eventID,ECount);
        mapREIndex.put(currEvent.eventID,REindex);
        mapPresentU.put(currEvent.eventID,presentU);
        mapScannedInPresentU.put(currEvent.eventID,scanned);
    }

    void FRJStarRJProcess(ArrayList<Integer> ROrder,HashMap<Integer,Integer> auxiliaryU2R,HashMap<Integer,HashSet<Integer>> auxiliaryR2Us,ArrayList<Integer> EOrder,HashMap<Integer,Integer> auxiliaryU2E,HashMap<Integer,HashSet<Integer>> auxiliaryE2Us,ArrayList<Pair> eventNum4partner,int duplicate,double KthValue){

        ArrayList<Integer> removed = new ArrayList<>();

        long start = System.currentTimeMillis();
        double tempTopk = 0;
        if(mapTempEP.size()>=param.queryTopK){
            tempTopk = FRJTempTopKth();
        }
        long end = System.currentTimeMillis();
        this.cal3 += (end-start);

        for(HashMap.Entry<Integer,HashMap<Integer,Double>> entry:mapEventsSimilarity.entrySet()){

            int currEvent = entry.getKey();

            if(duplicate == currEvent){
                continue;
            }

            ArrayList<Double> NSimilaritySumList = mapNSimilaritySumList.get(currEvent);
            HashSet<Integer> relatedEvents = mapRelatedEvents.get(currEvent);
            Double NDenominator = mapNDenominator.get(currEvent);
            HashMap<Integer,Double> eventsSimilarity = mapEventsSimilarity.get(currEvent);
            REIndex REindex = mapREIndex.get(currEvent);
            HashMap<Integer,Integer> RCount = mapRCount.get(currEvent);
            HashMap<Integer,Integer> ECount = mapECount.get(currEvent);
            HashSet<Integer> presentU = mapPresentU.get(currEvent);
            int scanned = mapScannedInPresentU.get(currEvent);

            double textualSimilarity = calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent));

            double pw = (KthValue - param.textualWeight * textualSimilarity) / (1 - param.textualWeight);
            int maxNum1NStar = 0;
            int maxNum1N = 0;
            if(ROrder.size() > REindex.k1){
                maxNum1NStar = ROrder.get(REindex.k1);
            }
            if(EOrder.size() > REindex.k2){
                maxNum1N = Math.min(relatedEvents.size(),EOrder.get(REindex.k2));
            }
            double pb = 0;
            if(maxNum1N != 0){
                pb = NSimilaritySumList.get(maxNum1N-1);
            }
            pb = (pb/NDenominator)*(maxNum1NStar/(double)attendedEvents.size());
            if (pw >= pb) {
                eventNumPruning++;
                Pair EP = mapTempEP.get(currEvent);
                if (EP.partnerID != -1) {
                    double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * EP.score;
                    topkEventPartnerQueue.add(new Pair(currEvent, EP.partnerID, FValue));
                }
                removed.add(currEvent);
                continue;
            }

            double pv = mapTempEP.get(currEvent).score;
            int finalPartner = mapTempEP.get(currEvent).partnerID;
            for(int i = step-augment; i < eventNum4partner.size() && i<step; i++){

                if (scanned == presentU.size()) {
                    eventNumPruning++;
                    if (finalPartner != -1) {
                        double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent, finalPartner, FValue));
                    }
                    removed.add(currEvent);
                    break;
                }

                int currMaxPartner = eventNum4partner.get(i).partnerID;
                if(!presentU.contains(currMaxPartner)){
                    continue;
                }

                int curr1 = 0;
                if(EOrder.size() > REindex.k2){
                    int currE = EOrder.get(REindex.k2);
                    curr1 = Math.min(relatedEvents.size(),currE);
                    while (!ECount.containsKey(currE) || ECount.get(currE) == 0){
                        REindex.k2++;
                        if(EOrder.size() <= REindex.k2){
                            curr1 = 0;
                            break;
                        }
                        currE = EOrder.get(REindex.k2);
                        curr1 = Math.min(relatedEvents.size(),currE);
                    }
                }

                int curr2 = 0;
                if(ROrder.size() > REindex.k1){
                    int currR = ROrder.get(REindex.k1);
                    curr2 = currR;
                    while (!RCount.containsKey(currR) || RCount.get(currR) == 0){
                        REindex.k1++;
                        if(ROrder.size() <= REindex.k1){
                            curr2 = 0;
                            break;
                        }
                        currR = ROrder.get(REindex.k1);
                        curr2 = currR;
                    }
                }

                double tmpMax = 0;
                if(curr1 != 0){
                    tmpMax = (NSimilaritySumList.get(curr1-1)/NDenominator)*(curr2/(double)attendedEvents.size());
                }
                if (pw >= tmpMax) {
                    eventNumPruning++;
                    if (finalPartner != -1) {
                        double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent, finalPartner, FValue));
                    }
                    removed.add(currEvent);
                    break;
                }
                if(pv >= tmpMax){
                    eventNumPruning++;
                    if(finalPartner != -1){
                        double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent,finalPartner,FValue));
                    }
                    removed.add(currEvent);
                    break;
                }
                if(tempTopk >= (param.textualWeight * textualSimilarity + param.togetherWeight * tmpMax)){
                    eventNumPruning++;
                    if (finalPartner != -1) {
                        double FValue = param.textualWeight * textualSimilarity + param.togetherWeight * pv;
                        topkEventPartnerQueue.add(new Pair(currEvent, finalPartner, FValue));
                    }
                    removed.add(currEvent);
                    break;
                }

                double currValue;
                double left;
                double right = 0;

                HashSet<Integer> events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
                events4currMaxPartner.retainAll(attendedEvents);
                left = events4currMaxPartner.size();

                events4currMaxPartner = new HashSet<>(userToEvent.get(currMaxPartner));
                events4currMaxPartner.retainAll(relatedEvents);
                Iterator<Integer> iterator = events4currMaxPartner.iterator();
                while(iterator.hasNext()){
                    int eventID = (Integer) iterator.next();
                    right+=eventsSimilarity.get(eventID);
                }

                currValue=(left/(double) attendedEvents.size())*(right/NDenominator);
                if(currValue>pv){
                    pv = currValue;
                    finalPartner = currMaxPartner;
                }

                if(auxiliaryU2R.containsKey(currMaxPartner)){
                    int currR = auxiliaryU2R.get(currMaxPartner);
                    int tmp = RCount.get(currR);
                    RCount.remove(currR);
                    RCount.put(currR,tmp-1);
                }
                if(auxiliaryU2E.containsKey(currMaxPartner)){
                    int currE = auxiliaryU2E.get(currMaxPartner);
                    int tmp = ECount.get(currE);
                    ECount.remove(currE);
                    ECount.put(currE,tmp-1);
                }

                userNumInvolved++;
                scanned++;
            }

            if(pv > mapTempEP.get(currEvent).score){
                mapTempEP.remove(currEvent);
                mapTempEP.put(currEvent,new Pair(-1,finalPartner,pv));
            }
            mapScannedInPresentU.remove(currEvent);
            mapScannedInPresentU.put(currEvent,scanned);
        }

        for(int i = 0;i<removed.size();i++){
            mapEventsSimilarity.remove(removed.get(i));
            mapNDenominator.remove(removed.get(i));
            mapNSimilaritySumList.remove(removed.get(i));
            mapRelatedEvents.remove(removed.get(i));
            mapTempEP.remove(removed.get(i));
            mapRCount.remove(removed.get(i));
            mapECount.remove(removed.get(i));
            mapREIndex.remove(removed.get(i));
            mapPresentU.remove(removed.get(i));
            mapScannedInPresentU.remove(removed.get(i));
        }
    }

    double FRJTempTopKth(){

        Comparator<Pair> customComparator = new PairScoreComparator();
        PriorityQueue<Pair> tempTopkQueue = new PriorityQueue<>(customComparator);

        for(HashMap.Entry<Integer,Pair> entry:mapTempEP.entrySet()){
            int currEvent = entry.getKey();
            double currPv = entry.getValue().score;
            double currValue = param.textualWeight * calculateQueryTextualSimilarity(param.queryObject,eventsInfo.get(currEvent)) + param.togetherWeight * currPv;
            tempTopkQueue.add(new Pair(currEvent,-1,currValue));
        }

        for(int i=0;i<param.queryTopK-1;i++){
            tempTopkQueue.poll();
        }

        return tempTopkQueue.peek().score;
    }

    double mem4SEUGEML = 0;
    double mem4U = 0;
    double cost4Usort = 0;

    double roundCount = 0;
    double partnerNumEachRound = 0;
    double eventNumEachRound = 0;

    void FNLJStar4case(String query,int k,double alpha) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);

        this.param.queryTopK = k;
        this.param.textualWeight = alpha;

        long start = System.currentTimeMillis();
        HashSet<Integer> historicalPartners = getHistoricalPartners();
        Iterator<Integer> iterator = historicalPartners.iterator();
        HashSet<Integer> totalEvents = new HashSet<>();
        while(iterator.hasNext()){
            totalEvents.addAll(userToEvent.get(iterator.next()));
        }
        HashMap<Integer,HashSet<Integer>> historicalPartnerInRelatedEvents = new HashMap<>();
        iterator = totalEvents.iterator();
        while(iterator.hasNext()){
            int currE = iterator.next();
            HashSet<Integer> currU = eventToUsers.get(currE);
            Iterator<Integer> it = currU.iterator();
            HashSet<Integer> historicalPartnerInSingleRelatedEvents = new HashSet<>();
            while (it.hasNext()){
                int user = it.next();
                if(historicalPartners.contains(user)){
                    historicalPartnerInSingleRelatedEvents.add(user);
                }
            }
            historicalPartnerInRelatedEvents.put(currE,historicalPartnerInSingleRelatedEvents);
        }

        HashMap<Integer,Integer> auxiliaryU2R = new HashMap<>();
        HashMap<Integer,HashSet<Integer>> auxiliaryR2Us = new HashMap<>();
        ArrayList<Integer> ROrder = new ArrayList<>();
        if(userToEvent.containsKey(param.queryUserID)){
            iterator = historicalPartners.iterator();
            while (iterator.hasNext()){
                int currUser = iterator.next();
                if(userToEvent.containsKey(currUser)){
                    HashSet<Integer> tmp = new HashSet<>(userToEvent.get(param.queryUserID));
                    tmp.retainAll(userToEvent.get(currUser));
                    int R = tmp.size();
                    auxiliaryU2R.put(currUser,R);
                    if(!auxiliaryR2Us.containsKey(R)){
                        auxiliaryR2Us.put(R,new HashSet<>());
                        ROrder.add(R);
                    }
                    auxiliaryR2Us.get(R).add(currUser);
                }
            }
        }
        IntegerComparator ic = new IntegerComparator();
        ROrder.sort(ic);

        HashMap<Integer,Integer> auxiliaryU2E = new HashMap<>();
        HashMap<Integer,HashSet<Integer>> auxiliaryE2Us = new HashMap<>();
        ArrayList<Integer> EOrder = new ArrayList<>();
        iterator = historicalPartners.iterator();
        while (iterator.hasNext()){
            int currUser = iterator.next();
            if(userToEvent.containsKey(currUser)){
                int E = userToEvent.get(currUser).size();
                auxiliaryU2E.put(currUser,E);
                if(!auxiliaryE2Us.containsKey(E)){
                    auxiliaryE2Us.put(E,new HashSet<>());
                    EOrder.add(E);
                }
                auxiliaryE2Us.get(E).add(currUser);
            }
        }
        EOrder.sort(ic);
        long end = System.currentTimeMillis();
        this.cost4Usort += (end-start);

        this.mem4SEUGEML += (ROrder.size()+EOrder.size()+auxiliaryU2E.size()*2+auxiliaryU2R.size()*2+auxiliaryR2Us.size()+auxiliaryE2Us.size());
        for(HashMap.Entry<Integer,HashSet<Integer>> entry : auxiliaryR2Us.entrySet()){
            this.mem4SEUGEML += entry.getValue().size();
        }
        for(HashMap.Entry<Integer,HashSet<Integer>> entry : auxiliaryE2Us.entrySet()){
            this.mem4SEUGEML += entry.getValue().size();
        }

        start = System.currentTimeMillis();
        ArrayList<Pair> eventNum4partner = new ArrayList<>();
        for(HashMap.Entry<Integer, Integer> entry: auxiliaryU2E.entrySet()) {
            int currUser = entry.getKey();
            int currE = auxiliaryU2E.get(currUser);
            int currR = auxiliaryU2R.get(currUser);
            eventNum4partner.add(new Pair(-1,currUser,currE*currR));
        }
        PairNumComparator customComparator = new PairNumComparator();
        eventNum4partner.sort(customComparator);
        end = System.currentTimeMillis();
        this.cost4Usort += (end-start);
        this.mem4U += eventNum4partner.size();

        while (!nextEventQueue.isEmpty()){

            roundCount++;

            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            if(!eventToUsers.containsKey(currEvent.eventID)) continue;

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            HashSet<Integer> relatedEvents = new HashSet<>();
            HashSet<Integer> presentU = new HashSet<>();
            ArrayList<Integer> candidateEvents;
            for (HashMap.Entry<String, Integer> entry : currEvent.wordsCount.entrySet()) {
                String currWord = entry.getKey();
                candidateEvents = InvertedIndex.get(currWord);
                for(int i=0;i<candidateEvents.size();i++){
                    int curr = candidateEvents.get(i);
                    if(totalEvents.contains(curr) && isNeighborhood(currEvent,eventsInfo.get(curr))){
                        relatedEvents.add(curr);
                        if(historicalPartnerInRelatedEvents.containsKey(curr)){
                            presentU.addAll(historicalPartnerInRelatedEvents.get(curr));
                        }
                    }
                }
            }
            eventNumEachRound+=relatedEvents.size();
            partnerNumEachRound+=presentU.size();

            HashMap<Integer,Integer> RCount = new HashMap<>();
            HashMap<Integer,Integer> ECount = new HashMap<>();
            iterator = presentU.iterator();
            while (iterator.hasNext()) {
                int currU = iterator.next();
                if(auxiliaryU2R.containsKey(currU)){
                    int currR = auxiliaryU2R.get(currU);
                    if(!RCount.containsKey(currR)){
                        RCount.put(currR,1);
                    }
                    else {
                        int tmp = RCount.get(currR);
                        RCount.remove(currR);
                        RCount.put(currR,tmp+1);
                    }
                }
                if(auxiliaryU2E.containsKey(currU)){
                    int currE = auxiliaryU2E.get(currU);
                    if(!ECount.containsKey(currE)){
                        ECount.put(currE,1);
                    }
                    else {
                        int tmp = ECount.get(currE);
                        ECount.remove(currE);
                        ECount.put(currE,tmp+1);
                    }
                }
            }

            FNLJStarPartnerComputation(currEvent,historicalPartners,relatedEvents,kthValue,ROrder,auxiliaryU2R,auxiliaryR2Us,EOrder,auxiliaryU2E,auxiliaryE2Us,eventNum4partner,RCount,ECount,presentU);
        }
        printTopkPairs4case("FNLJStar4case.txt",0);
    }

    void OriNLJ4case(String query,int k,double alpha) throws Exception{

        reset();
        setQuery(query);
        getAttendedEvent();
        generateNextEventsQueueWithII(param.queryObject);

        this.param.queryTopK = k;
        this.param.textualWeight = alpha;

        while (!nextEventQueue.isEmpty()){

            roundCount++;

            EventObject currEvent = eventsInfo.get(getNextEvent().eventID);

            if(!eventToUsers.containsKey(currEvent.eventID)) continue;

            double kthValue = 0;
            if(topkEventPartnerQueue.size()>=param.queryTopK){
                threshold = param.textualWeight*calculateQueryTextualSimilarity(param.queryObject,currEvent) + param.togetherWeight*1;
                kthValue = calculateTopKthValue();
                if(kthValue >= threshold) {
                    break;
                }
            }

            int finalPartner = -1;
            double togetherValue = 0;

            HashSet<Integer> relatedEvents = new HashSet<>();
            HashSet<Integer> candidateEvents = getRelatedEventFromInvertedIndex(currEvent);
            Iterator<Integer> iterator = candidateEvents.iterator();
            while(iterator.hasNext()){
                int eventID = iterator.next();
                if (isNeighborhood(currEvent, eventsInfo.get(eventID)) && userToEvent.get(param.queryUserID).contains(eventID)) relatedEvents.add(eventID);
            }
            eventNumEachRound+=relatedEvents.size();

            HashSet<Integer> partnerSet = new HashSet<>();
            iterator = relatedEvents.iterator();
            while(iterator.hasNext()){
                int eventID = iterator.next();
                partnerSet.addAll(eventToUsers.get(eventID));
            }
            partnerNumEachRound+=partnerSet.size();

            double denominator = 0;
            iterator = relatedEvents.iterator();
            while(iterator.hasNext()){
                int eventID = iterator.next();
                denominator += calculateTextualSimilarity(currEvent, eventsInfo.get(eventID));
            }
            if(denominator==0) continue;

            double nominator = 0;
            Iterator<Integer> outiterator = partnerSet.iterator();
            while (outiterator.hasNext()) {
                int currPartner = outiterator.next();
                iterator = userToEvent.get(currPartner).iterator();
                while (iterator.hasNext()) {
                    int curr_event = iterator.next();
                    if (relatedEvents.contains(curr_event)) {
                        nominator += calculateTextualSimilarity(currEvent, eventsInfo.get(curr_event));
                    }
                }
                if(togetherValue<(nominator / denominator)){
                    togetherValue = (nominator / denominator);
                    finalPartner = currPartner;
                }
            }

            if(finalPartner != -1){
                double textual_similarity = calculateQueryTextualSimilarity(param.queryObject,currEvent);
                double FValue = param.textualWeight*textual_similarity + param.togetherWeight*togetherValue;
                topkEventPartnerQueue.add(new Pair(currEvent.eventID,finalPartner,FValue));
            }
        }
        printTopkPairs4case("OriNLJ4case.txt",0);
    }

    void printTopkPairs4case(String filename, double rate) throws Exception{
        File file = new File(filename);
        if(!file.exists()){
            file.createNewFile();
        }

        StringBuilder str = new StringBuilder();
        str.append("userID: ").append(param.queryUserID).append("\nqueryKeywords: ");
        for(Map.Entry<String,Integer> entry : param.queryWordsCount.entrySet()){
            str.append(entry.getKey()).append(" ");
        }
        str.append("\n");

        /*
        int count = 0;
        for(int i=0;i<param.queryTopK;i++){
            Pair pair = topkEventPartnerQueue.poll();
            if(pair == null) break;
            if(eventToUsers.get(pair.eventID).contains(pair.partnerID)){
                str.append(pair.eventID).append(", ").append(pair.partnerID).append(", ");
                for(HashMap.Entry<String,Integer> entry : eventsInfo.get(pair.eventID).wordsCount.entrySet()){
                    str.append(entry.getKey()).append(" ");
                }
                str.append("\n");
                count++;
            }
        }
        str.append("\n");

        if(count>=param.queryTopK*rate){
            String line = str.toString();
            FileOutputStream out = null;
            byte[] buff = new byte[]{};
            out = new FileOutputStream(file,true);
            out.write(line.getBytes());
            out.flush();
            out.close();
        }
        */

        for(int i=0;i<param.queryTopK;i++){
            Pair pair = topkEventPartnerQueue.poll();
            if(pair == null) break;
            str.append(pair.eventID).append(", ").append(pair.partnerID).append(", ");
            for(HashMap.Entry<String,Integer> entry : eventsInfo.get(pair.eventID).wordsCount.entrySet()){
                str.append(entry.getKey()).append(" ");
            }
            str.append("\n");
        }

        str.append("relatedEventsNumEachRound:"+eventNumEachRound/roundCount+" partnersNumEachRound:"+partnerNumEachRound/roundCount+" roundCount:"+roundCount+"\n");
        str.append("\n");

        roundCount = 0;
        eventNumEachRound = 0;
        partnerNumEachRound = 0;

        String line = str.toString();
        FileOutputStream out = null;
        byte[] buff = new byte[]{};
        out = new FileOutputStream(file,true);
        out.write(line.getBytes());
        out.flush();
        out.close();
    }
}