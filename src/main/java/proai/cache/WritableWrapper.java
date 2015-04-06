package proai.cache;

import proai.Writable;
import proai.error.ServerException;

import java.io.PrintWriter;

public class WritableWrapper implements Writable {

    private String m_prependString;
    private String m_appendString;
    private Writable m_writable;

    public WritableWrapper(String prependString,
                           Writable writable,
                           String appendString) {

        m_prependString = prependString;
        m_writable = writable;
        m_appendString = appendString;

    }

    public void write(PrintWriter out) throws ServerException {
        out.print(m_prependString);
        out.flush();
        m_writable.write(out);
        out.print(m_appendString);
    }

}