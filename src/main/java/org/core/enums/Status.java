package org.core.enums;

public enum Status {
    UNSOLVED("unsolved"),
    SOLVED("solved"),
    PENDING("pending"),
    FAILED("failed"),
    IN_PROGRESS("in_progress"),
    ;

    private final String value;

    Status(String value) {
        this.value = value;
    }

    String getValue() {
        return value;
    }
}
