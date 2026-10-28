package dev.dimster.hooks;

/** A hook configured with {@code onFailure: FAIL} exited non-zero or timed out. */
public class HookFailedException extends RuntimeException {
    public HookFailedException(String message) {
        super(message);
    }
}
