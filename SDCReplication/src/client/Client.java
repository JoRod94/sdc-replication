package client;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by joaorodrigues on 14 Apr 16.
 */
public class Client {
    private ClientStub stub;

    public Client() throws IOException {
        this.stub = new ClientStub();
    }

    public void work() throws IOException, InterruptedException {
        Scanner scanner = new Scanner(System.in);

        String command;
        while(true){

            System.out.print("> ");

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
                result = "Invalid Command";
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
