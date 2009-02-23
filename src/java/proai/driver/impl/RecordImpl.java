package proai.driver.impl;

import java.io.File;

import proai.Record;

public class RecordImpl implements Record {

    private String m_itemID;
    private String m_prefix;
    private File m_file;

    public RecordImpl(String itemID, String prefix, File file) {
        m_itemID = itemID;
        m_prefix = prefix;
        m_file = file;
    }

    public String getItemID() {
        return m_itemID;
    }

    public String getPrefix() {
        return m_prefix;
    }

    public String getSourceInfo() {
        return m_file.getPath();
    }

}
