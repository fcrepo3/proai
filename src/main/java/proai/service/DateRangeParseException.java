package proai.service;

public class DateRangeParseException extends RuntimeException {

    public DateRangeParseException(String message) {
        super(message);

    }

    public DateRangeParseException(Throwable cause) {
        super(cause);
    }

    public DateRangeParseException(String message, Throwable cause) {
        super(message, cause);
    }

}