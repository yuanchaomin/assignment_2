/**
 * Created by yangyizhou on 2017/5/20.
 */
public class Point {
    Double ly6c;
    Double CD11b;
    Double SCA1;


    public Point(Double ly6c,Double CD11b,Double SCA1){
        this.ly6c = ly6c;
        this.CD11b = CD11b;
        this.SCA1 = SCA1;
    }

    public Point(){}


    public Double getLy6c() {
        return ly6c;
    }

    public void setLy6c(Double ly6c) {
        this.ly6c = ly6c;
    }

    public Double getCD11b() {
        return CD11b;
    }

    public void setCD11b(Double CD11b) {
        this.CD11b = CD11b;
    }

    public Double getSCA1() {
        return SCA1;
    }

    public void setSCA1(Double SCA1) {
        this.SCA1 = SCA1;
    }

    public double euclideanDistance(Point other) {
        return Math.sqrt((other.ly6c-ly6c)*(other.ly6c-ly6c) +
                (other.CD11b-CD11b)*(other.CD11b-CD11b)+(other.SCA1-SCA1)*(other.SCA1-SCA1));
    }

    @Override
    public String toString() {
        return "ly6c:"+ly6c+"\tCD11b:"+CD11b+"\tSCA1:"+SCA1;
    }

    public Point add(Point other) {
        ly6c += other.ly6c;
        CD11b += other.CD11b;
        SCA1 += other.SCA1;
        return this;
    }

    public Point div(long val) {
        ly6c /= val;
        CD11b /= val;
        SCA1 /= val;
        return this;
    }

    public void clear(){
        ly6c = CD11b = SCA1 = 0.0;
    }
}
