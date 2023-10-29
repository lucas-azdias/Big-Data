package advanced.tde2.exer03jhoel;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;







public class FlowYear implements WritableComparable<FlowYear> {


    private String year;

    private String Flow;


    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public FlowYear() {

    }


    public String getFlow() {
        return Flow;
    }

    public void setFlow(String flow) {
        Flow = flow;
    }

    public FlowYear(String Flow, String year) {

        this.Flow = Flow;
        this.year = year;
    }







    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowYear that = (FlowYear) o;
        return Objects.equals(year, that.year) && Objects.equals(Flow, that.Flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, Flow);
    }

    @Override
    public String toString() {
        return  year+ "\t" +Flow;
    }

    @Override
    public int compareTo(FlowYear o) {
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
        dataOutput.writeUTF(year);

        dataOutput.writeUTF(Flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readUTF();


        Flow = dataInput.readUTF();
    }
}