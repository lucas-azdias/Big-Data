package advanced.tde2.exer07azevedo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommodityFlow implements WritableComparable<CommodityFlow> {

    private String commodity;
    private String flow;

    public CommodityFlow() {
        this.commodity = null;
        this.flow = null;
    }

    public CommodityFlow(String commodity, String flow) {
        this.commodity = commodity;
        this.flow = flow;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommodityFlow that = (CommodityFlow) o;
        return Objects.equals(commodity, that.commodity) && Objects.equals(flow, that.flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, flow);
    }

    @Override
    public String toString() {
        return commodity + "\t" + flow;
    }

    @Override
    public int compareTo(CommodityFlow o) {
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
        dataOutput.writeUTF(flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        flow = dataInput.readUTF();
    }
}
