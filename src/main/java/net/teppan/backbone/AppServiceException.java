package net.teppan.backbone;

/**
 * Checked exception thrown when an {@link AppService} fails.
 *
 * <p>Use {@code AppServiceException} for recoverable failures that the caller
 * can meaningfully handle — for example, validation errors or expected business
 * rule violations. Programming errors should propagate as unchecked exceptions.
 */
public class AppServiceException extends Exception {

    /**
     * Constructs an {@code AppServiceException} with the given detail message.
     *
     * @param message the detail message
     */
    public AppServiceException(String message) {
        super(message);
    }

    /**
     * Constructs an {@code AppServiceException} wrapping an underlying cause.
     *
     * @param message the detail message
     * @param cause   the root cause
     */
    public AppServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an {@code AppServiceException} whose message is derived from
     * the given cause.
     *
     * @param cause the root cause
     */
    public AppServiceException(Throwable cause) {
        super(cause);
    }
}
