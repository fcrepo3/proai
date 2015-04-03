package proai.error;

public class BadVerbException extends ProtocolException {
	static final long serialVersionUID = 1;
	
    public BadVerbException() {
        super("Value of the verb argument is not a legal OAI-PMH verb, the verb argument is missing, or the verb argument is repeated.");
    }

    public BadVerbException(String message) {
        super(message);
    }

    public BadVerbException(String message,
                             Throwable cause) {
        super(message, cause);
    }

    public String getCode() { return "badVerb"; }

}