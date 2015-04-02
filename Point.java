/**
 * Created by ramyashenoy on 12/6/14.
 */
public class Point{

    Double x1;
    Double y1;

    public Point(Double x1, Double y1) {
        this.x1 = x1;
        this.y1 = y1;
    }

    public Double getX1() {
        return x1;
    }

    public void setX1(Double x1) {
        this.x1 = x1;
    }

    public Double getY1() {
        return y1;
    }

    public void setY1(Double y1) {
        this.y1 = y1;
    }

    @Override
    public String toString() {
        return x1 + "," + y1;
    }

    public double distanceTo(Point two){
        return Math.sqrt(Math.pow((two.getX1() - this.x1), 2) - Math.pow((two.getY1()- this.y1), 2));
    }
}
