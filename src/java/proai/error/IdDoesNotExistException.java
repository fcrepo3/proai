package proai.error;

public class IdDoesNotExistException extends ProtocolException {
	static final long serialVersionUID = 1;
	
    public IdDoesNotExistException() {
        super("The value of the identifier argument is unknown or illegal in this repository.");
    }

    public IdDoesNotExistException(String message) {
        super(message);
    }

    public IdDoesNotExistException(String message,
                             Throwable cause) {
        super(message, cause);
    }

    public String getCode() { return "idDoesNotExist"; }

}