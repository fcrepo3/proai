package proai.error;

/**
 * Signals an unexpected condition with the server.
 */
public class ServerException extends RuntimeException {

    public ServerException(String message) {
        super(message);
    }

    public ServerException(String message,
                               Throwable cause) {
        super(message, cause);
    }

}