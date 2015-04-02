package proai.error;

public class NoMetadataFormatsException extends ProtocolException {
	static final long serialVersionUID = 1;
	
    public NoMetadataFormatsException() {
        super("There are no metadata formats available for the specified item.");
    }

    public NoMetadataFormatsException(String message) {
        super(message);
    }

    public NoMetadataFormatsException(String message,
                             Throwable cause) {
        super(message, cause);
    }

    public String getCode() { return "noMetadataFormats"; }

}