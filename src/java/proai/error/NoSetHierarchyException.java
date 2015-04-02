package proai.error;

public class NoSetHierarchyException extends ProtocolException {
	static final long serialVersionUID = 1;
	
    public NoSetHierarchyException() {
        super("The repository does not support sets.");
    }

    public NoSetHierarchyException(String message) {
        super(message);
    }

    public NoSetHierarchyException(String message,
                             Throwable cause) {
        super(message, cause);
    }

    public String getCode() { return "noSetHierarchy"; }

}