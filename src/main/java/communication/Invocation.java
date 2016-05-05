package communication;

import java.io.*;

/**
 * Created by frm on 22/02/16.
 * Class representing the remote invocation to be processed by the server
 */
public class Invocation implements Serializable {
    // When making an RPC using this class,
    // use these variables to ensure the methods are supported.
    // Allows for code compatibility if the underlying method changes
    public static final String CREATE   = "create";
    public static final String BALANCE  = "balance";
    public static final String MOVEMENT = "movement";
    public static final String TRANSFER = "transfer";
    public static final String STATE    = "state";
    public static final String LATEST   = "latest";

    private String command;
    private Object[] args;

    /**
     * Creates an invocation with the given command and its arguments
     * @param command - command to be invoked remotely. Should be one of the provided class commands.
     * @param args - arguments to the remote method invocation
     */
    public Invocation(String command, Object[] args) {
        this.command = command;
        this.args = args;
    }

    /**
     * @return - command to be invoked
     */
    public String getCommand() {
        return command;
    }

    /**
     * @return - command arguments
     */
    public Object[] getArgs() {
        return args;
    }
}
