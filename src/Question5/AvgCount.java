package Question5;

import java.io.Serializable;

public class AvgCount implements Serializable {

    private int count;
    private double price;

    public AvgCount() {
    }

    public AvgCount(int count, double price) {
        this.count = count;
        this.price = price;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "AvgCount{" +
                "count=" + count +
                ", price=" + price +
                '}';
    }
}