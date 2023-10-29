package advanced.tde2.exer06jhoel;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class Composto implements WritableComparable<Composto> {
    private long commodityID;
    private String commodity;
    private String category;

    private String Flow;
    private double price;




    public Composto() {

    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getFlow() {
        return Flow;
    }

    public void setFlow(String flow) {
        Flow = flow;
    }

    public Composto(String commodity, String Flow, String category, long commodityID , double price) {
        this.commodityID = commodityID;
        this.price = price;
        this.commodity = commodity;
        this.Flow = Flow;
        this.category = category;
    }

    public long getCommodityID() {
        return commodityID;
    }

    public void setCommodityID(long commodityID) {
        this.commodityID = commodityID;
    }

    public double getPrice(){
        return price;
    }
    public void setPrice(double price){
        this.price = price;

    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Composto that = (Composto) o;
        return Objects.equals(category, that.category) && Objects.equals(commodity, that.commodity) && Objects.equals(price, that.price)&& Objects.equals(commodityID, that.commodityID)&& Objects.equals(Flow, that.Flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, commodity, price,commodityID,Flow);
    }

    @Override
    public String toString() {
        return category + "\t" + commodity + "\t" + price + "\t" + commodityID+ "\t" +Flow;
    }

    @Override
    public int compareTo(Composto o) {
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
        dataOutput.writeUTF(category);
        dataOutput.writeUTF(commodity);
        dataOutput.writeDouble(price);
        dataOutput.writeLong(commodityID);
        dataOutput.writeUTF(Flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        category = dataInput.readUTF();
        commodity = dataInput.readUTF();
        price = dataInput.readDouble();
        commodityID = dataInput.readLong();
        Flow = dataInput.readUTF();
    }
}