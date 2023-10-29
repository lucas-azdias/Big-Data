package advanced.tde2.exer06jhoel;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;



public class KeyYearCategory implements WritableComparable<KeyYearCategory> {

    private String category;

    private String Year;





    public KeyYearCategory() {

    }



    public KeyYearCategory(String Year, String category) {
        this.Year = Year;
        this.category = category;
    }

    public String getYear() {
        return Year;
    }

    public void setYear(String year) {
        Year = year;
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
        KeyYearCategory that = (KeyYearCategory) o;
        return Objects.equals(category, that.category)  && Objects.equals(Year, that.Year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, Year);
    }

    @Override
    public String toString() {
        return category + "\t" + Year ;
    }

    @Override
    public int compareTo(KeyYearCategory o) {
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
        dataOutput.writeUTF(Year);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        category = dataInput.readUTF();
        Year = dataInput.readUTF();

    }
}