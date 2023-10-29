package advanced.tde2.exer07azevedo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommodityAmount implements WritableComparable<CommodityAmount> {

    private String commodity;
    private long amount;

    public CommodityAmount() {
        this.commodity = null;
        this.amount = 0;
    }

    public CommodityAmount(String commodity, long amount) {
        this.commodity = commodity;
        this.amount = amount;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommodityAmount that = (CommodityAmount) o;
        return Objects.equals(commodity, that.commodity) && Objects.equals(amount, that.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, amount);
    }

    @Override
    public String toString() {
        return commodity + "\t" + amount;
    }

    @Override
    public int compareTo(CommodityAmount o) {
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
        dataOutput.writeLong(amount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        amount = dataInput.readLong();
    }
}
