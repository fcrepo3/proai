package proai.error;

public class RepositoryException extends ServerException {

    public RepositoryException(String message) {
        super(message);
    }

    public RepositoryException(String message,
                               Throwable cause) {
        super(message, cause);
    }

}