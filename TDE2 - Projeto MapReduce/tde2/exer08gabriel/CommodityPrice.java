package advanced.tde2.exer08gabriel;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommodityPrice implements WritableComparable<CommodityPrice> {

    private String commodity;
    private double price;

    public CommodityPrice() {
        this.commodity = null;
        this.price = 0;
    }

    public CommodityPrice(String commodity, double price) {
        this.commodity = commodity;
        this.price = price;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommodityPrice that = (CommodityPrice) o;
        return Objects.equals(commodity, that.commodity) && Objects.equals(price, that.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, price);
    }

    @Override
    public String toString() {
        return commodity + "\t" + price;
    }

    @Override
    public int compareTo(CommodityPrice o) {
        if (hashCode() < o.hashCode()) {
            return -1;
        } else if (hashCode() > o.hashCode()) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        price = dataInput.readDouble();
    }
}
