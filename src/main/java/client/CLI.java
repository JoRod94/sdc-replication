package client;

import bank.Bank;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by joaorodrigues on 14 Apr 16.
 */
public class CLI {
    private final static String DEFAULT_REPLY = "Invalid Command";
    private final static String DEFAULT_BALANCE_ERROR_MSG = "Account doesn't exist";
    private final static String INVALID_AMOUNT_TRANSFER = "Invalid amount for transfer";
    private Bank stub;

    public CLI() throws IOException {
        this.stub = new BankStub();
    }

    /**
     * Runs the client
     * @throws IOException
     * @throws InterruptedException
     */
    public void work() throws IOException, InterruptedException {
        Scanner scanner = new Scanner(System.in);
        String command;

        while(true){
            System.out.println("Write Parameters: ");

            command = scanner.nextLine();

            interpret(command);
        }
    }

    /**
     * Interprets a given command and its arguments, invoking the corresponding stub method
     * @param command - list composed of the actual command (first element) and its arguments
     * @throws IOException
     */
    private void interpret(String command) throws IOException {
        String args[] = command.split(" ");
        Object result = DEFAULT_REPLY;

        switch(args[0]) {
            case "create":
                if (args.length == 1)
                    result = stub.create();
                break;
            case "balance":
                if(args.length == 2){
                    result = stub.balance(args[1]);
                    if(result == null) result = DEFAULT_BALANCE_ERROR_MSG;
                }
                break;
            case "movement":
                if (args.length == 3)
                    result = stub.movement(args[1], Integer.parseInt(args[2]));
                break;
            case "transfer":
                if (args.length == 4) {
                    if(Integer.parseInt(args[3]) < 0)
                        result = INVALID_AMOUNT_TRANSFER;
                    else
                        result = stub.transfer(args[1], args[2], Integer.parseInt(args[3]));
                }
                break;
            case "latest":
                if(args.length == 3){
                    result = stub.latest(args[1], Integer.parseInt(args[2]));
                    if(result == null) result = DEFAULT_BALANCE_ERROR_MSG;
                }
                break;
            default:
                result = DEFAULT_REPLY;
                break;
        }

        System.out.println(result);
    }

    public static void main(String[] args){
        try {
            (new CLI()).work();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
