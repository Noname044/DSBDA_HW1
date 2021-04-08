package bdtc.lab1;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Custom writable comparable Int
 */
public class CustomInt implements WritableComparable<CustomInt>{
    private int errorCode;

    /**
     * Empty constructor
     */
    public CustomInt() { }

    /**
     * Constructor with value
     */
    public CustomInt(int parseInt) { errorCode = parseInt; }

    /**
     * Override write
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException { dataOutput.writeInt(this.errorCode); }

    /**
     * Override readFields
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException { this.errorCode = dataInput.readInt(); }

    /**
     * Getter
     */
    public int get() { return this.errorCode; }

    /**
     * Override compareTo
     */
    @Override
    public int compareTo(CustomInt o) { return Integer.compare(o.get(), this.errorCode); }

    /**
     * Override equals
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CustomInt)) {
            return false;
        } else {
            CustomInt other = (CustomInt)o;
            return this.errorCode == other.errorCode;
        }
    }

    /**
     * Override hashCode
     */
    @Override
    public int hashCode() {
        return this.errorCode;
    }

    /**
     * Override toString
     */
    @Override
    public String toString() {
        return Integer.toString(this.errorCode);
    }

}
