package proai.driver.impl;

import proai.MetadataFormat;

public class MetadataFormatImpl implements MetadataFormat {

    private String m_prefix;
    private String m_namespaceURI;
    private String m_schemaLocation;

    public MetadataFormatImpl(String prefix,
                              String namespaceURI,
                              String schemaLocation) {
        m_prefix = prefix;
        m_namespaceURI = namespaceURI;
        m_schemaLocation = schemaLocation;
    }

    public String getPrefix() {
        return m_prefix;
    }

    public String getNamespaceURI() {
        return m_namespaceURI;
    }

    public String getSchemaLocation() {
        return m_schemaLocation;
    }

}