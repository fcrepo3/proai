package proai.error;

public class CannotDisseminateFormatException extends ProtocolException {

    public CannotDisseminateFormatException() {
        super("The metadata format identified by the value given for the metadataPrefix argument is not supported by the item or by the repository.");
    }

    public CannotDisseminateFormatException(String message) {
        super(message);
    }

    public CannotDisseminateFormatException(String message,
                             Throwable cause) {
        super(message, cause);
    }

    public String getCode() { return "cannotDisseminateFormat"; }

}