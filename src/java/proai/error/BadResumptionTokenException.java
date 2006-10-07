package proai.error;

public class BadResumptionTokenException extends ProtocolException {

    public BadResumptionTokenException() {
        super("The value of the resumptionToken argument is invalid or expired.");
    }

    public BadResumptionTokenException(String message) {
        super(message);
    }

    public BadResumptionTokenException(String message,
                             Throwable cause) {
        super(message, cause);
    }

    public String getCode() { return "badResumptionToken"; }

}