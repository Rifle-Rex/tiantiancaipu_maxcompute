package customerException;

public class IgnoreRecordException extends Exception {
    public IgnoreRecordException(String errorMessage) {
        super(errorMessage);
    }
}
