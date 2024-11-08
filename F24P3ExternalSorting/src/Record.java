/**
 * Holds a single record
 *
 * @author CS Staff
 * @version Fall 2024
 */
public class Record
    implements Comparable<Record>
{
    /**
     * 16 bytes per record
     */
    public static final int BYTES = 16;

    private long            recID;
    private double          key;
    private int             runNo;

    /**
     * The constructor for the Record class
     *
     * @param recID
     *            record ID
     * @param key
     *            record key
     */
    public Record(long recID, double key)
    {
        this.recID = recID;
        this.key = key;
        runNo = -1;
    }


    // ----------------------------------------------------------
    /**
     * Return the ID value from the record
     *
     * @return record ID
     */
    public long getID()
    {
        return recID;
    }


    // ----------------------------------------------------------
    /**
     * Return the key value from the record
     *
     * @return record key
     */
    public double getKey()
    {
        return key;
    }

    /**
     * set run number
     * @param r run number
     */
    public void setRunNo(int r) {
        runNo = r;
    }
   
    /**
     * get run number
     * @return int - run number
     */
    public int getRunNo() {
        return runNo;
    }


    // ----------------------------------------------------------
    /**
     * Compare two records based on their keys
     *
     * @return int
     */
    @Override
    public int compareTo(Record toBeCompared)
    {
        return Double.compare(this.key, toBeCompared.key);
    }
}
