import java.util.Comparator;

class Pair{
    int eventID;
    int partnerID;
    int num;
    double score;

    Pair(int eventID,int partnerID,double score){
        this.eventID=eventID;
        this.partnerID=partnerID;
        this.score=score;
    }

    Pair(int eventID,int partnerID,int num){
        this.eventID=eventID;
        this.partnerID=partnerID;
        this.num=num;
    }
}

class PairScoreComparator implements Comparator<Pair> {
    @Override
    public int compare(Pair x, Pair y) {
        if (x.score < y.score) {
            return 1;
        } else if (x.score > y.score) {
            return -1;
        }
        return 0;
    }
}

class PairNumComparator implements Comparator<Pair> {
    @Override
    public int compare(Pair x, Pair y) {
        if (x.num < y.num) {
            return 1;
        } else if (x.num > y.num) {
            return -1;
        }
        return 0;
    }
}

class DoubleComparator implements Comparator<Double> {
    @Override
    public int compare(Double x, Double y) {
        if (x < y) {
            return 1;
        } else if (x > y) {
            return -1;
        }
        return 0;
    }
}

class IntegerComparator implements Comparator<Integer> {
    @Override
    public int compare(Integer x, Integer y) {
        if (x < y) {
            return 1;
        } else if (x > y) {
            return -1;
        }
        return 0;
    }
}

class REIndex{
    int k1;
    int k2;

    REIndex(){
        this.k1 = 0;
        this.k2 = 0;
    }
}