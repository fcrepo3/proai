package proai.error;

public abstract class ProtocolException extends ServerException {

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message,
                             Throwable cause) {
        super(message, cause);
    }

    /**
     * Get the error code of this exception, as defined by the OAI-PMH.
     */
    public abstract String getCode();

}
