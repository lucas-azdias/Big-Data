package advanced.tde2.exer05victor;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class YearCategoryUnitType implements WritableComparable<YearCategoryUnitType> {
    private int year;
    private String category;
    private String unitType;

    public YearCategoryUnitType(){
        this.year = 0;
        this.category = null;
        this.unitType = null;
    }
    public YearCategoryUnitType(int year, String category, String unitType) {
        this.year = year;
        this.category = category;
        this.unitType = unitType;
    }

    public String getUnitType() {
        return unitType;
    }

    public void setUnitType(String unitType) {
        this.unitType = unitType;
    }


    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearCategoryUnitType that = (YearCategoryUnitType) o;
        return year == that.year && Objects.equals(category, that.category) && Objects.equals(unitType, that.unitType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, category);
    }
    @Override
    public String toString() {
        return year + "\t" + category + "\t" + unitType;
    }
    @Override
    public int compareTo(YearCategoryUnitType o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeUTF(category);
        dataOutput.writeUTF(unitType);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        category = dataInput.readUTF();
        unitType = dataInput.readUTF();
    }


}