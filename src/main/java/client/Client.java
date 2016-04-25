package client;

import bank.Bank;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by joaorodrigues on 14 Apr 16.
 */
public class Client {
    private final static String DEFAULT_REPLY = "Invalid Command";
    private final static String DEFAULT_BALANCE_ERROR_MSG = "Account doesn't exist";
    private Bank stub;

    public Client() throws IOException {
        this.stub = new BankStub();
    }

    public void work() throws IOException, InterruptedException {
        Scanner scanner = new Scanner(System.in);
        String command;

        while(true){
            System.out.println("Write Parameters: ");

            command = scanner.nextLine();

            interpret(command);
        }
    }

    // TODO: Decidir se é preciso meter mais mensagens de controlo de erros
    public void interpret(String command) throws IOException {
        String args[] = command.split(" ");
        Object result = null;

        switch(args[0]){
            case "create":
                if(args.length == 1)
                    result = stub.create();
                break;
            case "balance":
                if(args.length == 2)
                    result = stub.balance(args[1]);
                    if(result == null) result = DEFAULT_BALANCE_ERROR_MSG;
                break;
            case "movement":
                if(args.length == 3)
                    result = stub.movement(args[1], Integer.parseInt(args[2]));
                break;
            case "transfer":
                if(args.length == 4)
                    result = stub.transfer(args[1], args[2],
                                            Integer.parseInt(args[3]));
                break;
            case "movements":
                //TODO
            default:
                result = DEFAULT_REPLY;
                break;
        }

        System.out.println(result);
    }

    public static void main(String[] args){
        try {
            (new Client()).work();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
