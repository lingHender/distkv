package org.ctp.domian;

/**
 * Created by lfli on 22/08/2018.
 */
public class Result {
    private String message;
    private boolean endClient;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isEndClient() {
        return endClient;
    }

    public void setEndClient(boolean endClient) {
        this.endClient = endClient;
    }
}
