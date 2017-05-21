/**
 * Created by yangyizhou on 2017/5/20.
 */
public class Center extends Point {
    int clusterID;

    public Center() {
    }

    public Center(Double ly6c, Double CD11b, Double SCA1, int clusterID) {
        super(ly6c, CD11b, SCA1);
        this.clusterID = clusterID;
    }

    public Center(int clusterID,Point p){
        super(p.ly6c,p.CD11b,p.SCA1);
        this.clusterID=clusterID;
    }

    @Override
    public String toString() {
        return clusterID+" "+super.toString();
    }

    public int getClusterID() {
        return clusterID;
    }

    public void setClusterID(int clusterID) {
        this.clusterID = clusterID;
    }
}
