package advanced.tde2.exer04victor;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
public class AvgPrice implements WritableComparable<AvgPrice> {
    float price;
    int ocurrence;

    public AvgPrice(float price, int ocurrence) {
        this.price = price;
        this.ocurrence = ocurrence;
    }
    public AvgPrice(){
        this.price = 0;
        this.ocurrence = 0;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getOcurrence() {
        return ocurrence;
    }

    public void setOcurrence(int ocurrence) {
        this.ocurrence = ocurrence;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvgPrice that = (AvgPrice) o;
        return Float.compare(that.price, price) == 0 &&
                ocurrence == that.ocurrence;
    }
    @Override
    public int hashCode() {
        return Objects.hash(price, ocurrence);
    }
    @Override
    public String toString() {
        return super.toString();
    }
    @Override
    public int compareTo(AvgPrice o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(price);
        dataOutput.writeInt(ocurrence);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        price = dataInput.readFloat();
        ocurrence = dataInput.readInt();
    }
}

