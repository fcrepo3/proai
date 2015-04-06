package proai.error;

/**
 * Signals that an immediate shutdown has been requested.
 */
public class ImmediateShutdownException extends RuntimeException {
    static final long serialVersionUID = 1;

    public ImmediateShutdownException() {
        super();
    }

}