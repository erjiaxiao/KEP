
import java.util.HashMap;

class Timer {
    private HashMap<String,Long> duration;
    private HashMap<String,Long> count;
    private HashMap<String,Long> startTime;

    Timer(){
        this.duration = new HashMap<>();
        this.count = new HashMap<>();
        this.startTime = new HashMap<>();
    }

    void startTiming(String type){

        if(!duration.containsKey(type)){
            this.duration.put(type, 0L);
            this.count.put(type, 0L);
        }
        Long start = System.currentTimeMillis();
        this.startTime.put(type,start);
    }

    void stopTiming(String type){
        Long end = System.currentTimeMillis();
        Long start = this.startTime.get(type);
        long currDuration = this.duration.get(type);
        this.duration.remove(type);
        this.duration.put(type,currDuration + (end - start));

        long currCount = this.count.get(type);
        this.count.remove(type);
        this.count.put(type,currCount+1);
    }

    void printTime(){
        for(HashMap.Entry<String, Long> entry: this.duration.entrySet())
        {
            String type = entry.getKey();
            if(this.count.get(type)==0) {
                continue;
            }
            long time = entry.getValue();
            System.out.println(type+":"+time+"ms");
        }
        System.out.println();
    }
}
