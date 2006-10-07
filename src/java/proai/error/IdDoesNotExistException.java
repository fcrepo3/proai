package proai.error;

public class IdDoesNotExistException extends ProtocolException {

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