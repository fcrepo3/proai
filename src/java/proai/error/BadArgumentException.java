package proai.error;

public class BadArgumentException extends ProtocolException {
	static final long serialVersionUID = 1;
	
    public BadArgumentException() {
        super("The request includes illegal arguments, is missing required arguments, includes a repeated argument, or values for arguments have an illegal syntax.");
    }

    public BadArgumentException(String message) {
        super(message);
    }

    public BadArgumentException(String message,
                             Throwable cause) {
        super(message, cause);
    }

    public String getCode() { return "badArgument"; }

}