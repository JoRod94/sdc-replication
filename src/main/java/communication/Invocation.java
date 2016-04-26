package communication;

import java.io.*;

/**
 * Created by frm on 22/02/16.
 */
public class Invocation implements Serializable {
    // When making an RPC using the invocation class
    // use these variables to ensure the methods are supported.
    // Also allows for code compatibility if the method changes
    public static final String CREATE   = "create";
    public static final String BALANCE  = "balance";
    public static final String MOVEMENT = "movement";
    public static final String TRANSFER = "transfer";
    public static final String STATE    = "state";
    public static final String LATEST   = "latest";

    private String command;
    private Object[] args;

    public Invocation(String command, Object[] args) {
        this.command = command;
        this.args = args;
    }

    public String getCommand() {
        return command;
    }

    public Object[] getArgs() {
        return args;
    }
}
