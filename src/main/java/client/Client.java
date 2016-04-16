package client;

import bank.Bank;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by joaorodrigues on 14 Apr 16.
 */
public class Client {
    private final static String DEFAULT_REPLY = "Invalid Command";
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

    public void interpret(String command) throws IOException {
        String args[] = command.split(" ");
        Object result = null;

        switch(args[0]){
            case "balance":
                if(args.length == 1)
                    result = stub.balance();
                break;
            case "movement":
                if(args.length == 2)
                    result = stub.movement(Integer.parseInt(args[1]));
                break;
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
