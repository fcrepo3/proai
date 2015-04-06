package proai.util;

import java.util.List;

/**
 * Interface for a converter of TableSpec objects to
 * RDBMS-specific DDL code.</p>
 * <p/>
 * <p>Implementations of this class must be thread-safe.</p>
 * <p>Implementations must also have a public no-arg constructor.</p>
 */
public interface DDLConverter {

    public boolean supportsTableType();

    public List<String> getDDL(TableSpec tableSpec);

    public String getDropDDL(String command);

}

