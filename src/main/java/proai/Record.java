package proai;


public interface Record {

    String getItemID();

    String getPrefix();

    /**
     * Get a string that can be used to construct the XML of the record.
     * <p/>
     * The format of this string is defined by the implementation.
     * <p/>
     * The string will typically contain some kind of identifier or locator
     * (a file path or URL) and possibly, other attributes that may be used
     * to construct a record's XML.
     */
    String getSourceInfo();

}
