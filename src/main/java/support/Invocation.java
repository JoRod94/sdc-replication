package support;

import java.io.*;

/**
 * Created by frm on 22/02/16.
 */
public class Invocation implements Serializable{
    public String command;
    public Object[] args;

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
