package proai.error;

public class NoRecordsMatchException extends ProtocolException {
	static final long serialVersionUID = 1;
	
    public NoRecordsMatchException() {
        super("The combination of the values of the from, until, set and metadataPrefix arguments results in an empty list.");
    }

    public NoRecordsMatchException(String message) {
        super(message);
    }

    public NoRecordsMatchException(String message,
                             Throwable cause) {
        super(message, cause);
    }

    public String getCode() { return "noRecordsMatch"; }

}