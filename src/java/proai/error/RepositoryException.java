package proai.error;

public class RepositoryException extends ServerException {
	static final long serialVersionUID = 1;
	
    public RepositoryException(String message) {
        super(message);
    }

    public RepositoryException(String message,
                               Throwable cause) {
        super(message, cause);
    }

}