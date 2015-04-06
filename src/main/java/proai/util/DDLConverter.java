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

    boolean supportsTableType();

    List<String> getDDL(TableSpec tableSpec);

    String getDropDDL(String command);

}

