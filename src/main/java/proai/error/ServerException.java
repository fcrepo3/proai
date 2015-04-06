package proai.error;

/**
 * Signals an unexpected condition with the server.
 */
public class ServerException extends RuntimeException {
    static final long serialVersionUID = 1;

    public ServerException(String message) {
        super(message);
    }

    public ServerException(String message,
                           Throwable cause) {
        super(message, cause);
    }

}