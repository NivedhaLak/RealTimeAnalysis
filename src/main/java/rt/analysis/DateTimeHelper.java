package rt.analysis;

public class DateTimeHelper {
    public static long getDifferenceInSecond(long srtTime , long endTime){
        return (endTime - srtTime)/1000;
    }
}
