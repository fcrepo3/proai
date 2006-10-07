package proai.error;

/**
 * Signals that an immediate shutdown has been requested.
 */
public class ImmediateShutdownException extends RuntimeException {

    public ImmediateShutdownException() {
        super();
    }

}