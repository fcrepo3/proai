package proai.service;

import proai.Writable;
import proai.error.ServerException;

import java.io.PrintWriter;

public class ResponseDataImpl implements ResponseData {

    private Writable m_writable;
    private String m_resumptionToken;

    public ResponseDataImpl(Writable writable) {
        m_writable = writable;
        m_resumptionToken = null;
    }

    public ResponseDataImpl(Writable writable, String resumptionToken) {
        m_writable = writable;
        m_resumptionToken = resumptionToken;
    }

    public void write(PrintWriter out) throws ServerException {
        m_writable.write(out);
    }

    public String getResumptionToken() {
        return m_resumptionToken;
    }

    public void release() {
    }

}
