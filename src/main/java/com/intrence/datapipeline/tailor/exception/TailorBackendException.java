package com.intrence.datapipeline.tailor.exception;

public class TailorBackendException extends Exception{
    private String detailMessage;
    private String identifier;

    public TailorBackendException(Throwable t) {
        super(t);
    }

    public TailorBackendException(String message, Throwable t) {
        super(message, t);
    }

    public TailorBackendException(String message) {
        super(message);
    }

    public TailorBackendException(String message, String identifier) {
        super(message);
        setIdentifier(identifier);
    }

    public void setMessage(String message) {
        this.detailMessage = message;
    }

    public String getMessage() {
        if (this.detailMessage == null) {
            return this.identifier + ":" + super.getMessage();
        }
        return this.detailMessage;
    }

    public String getIdentifier() {
        return this.identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

}
