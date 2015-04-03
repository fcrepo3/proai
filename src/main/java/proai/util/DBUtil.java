package proai.util;

import java.sql.*;

public abstract class DBUtil {

    /**
     * Get a long string, which could be a TEXT or CLOB type.
     * (CLOBs require special handling -- this method normalizes the reading of them)
     */
    public static String getLongString(ResultSet rs, int pos) throws SQLException {
        String s = rs.getString(pos);
        if (s != null) {
            // It's a String-based datatype, so just return it.
            return s;
        } else {
            // It may be a CLOB.  If so, return the contents as a String.
            try {
                Clob c = rs.getClob(pos);
                return c.getSubString(1, (int) c.length());
            } catch (Throwable th) {
                th.printStackTrace();
                return null;
            }
        }
    }

    public static String quotedString(String in, 
                                      boolean backslashIsEscape) {
        StringBuffer out = new StringBuffer();
        out.append('\'');
        if (in == null) {
            out.append("null");
        } else {
            for (int i = 0; i < in.length(); i++) {
                char c = in.charAt(i);
                if (c == '\'') {
                    out.append("''");                           //  ' ==> ''
                } else if (backslashIsEscape && c == '\\') {
                    out.append("\\\\");                         //  \ ==> \\
                } else {
                    out.append(c);
                }
            }
        }
        out.append('\'');
        return out.toString();
    }
}