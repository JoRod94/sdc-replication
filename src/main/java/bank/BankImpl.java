package bank;

import data.DataAccess;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by joaorodrigues on 14 Apr 16.
 */
public class BankImpl implements Bank, Serializable {
    private TreeMap<String, Integer> accounts;
    private int id;
    private DataAccess database;

    public BankImpl() {
        accounts = new TreeMap<>();
        id = 0;
    }

    //Deprecated constructor
    public BankImpl(BankImpl b) {
        System.out.println("RECOVERING STATE...");
        accounts = b.accounts;
        id = b.id;
        for(Map.Entry<String, Integer> p : accounts.entrySet())
            System.out.println("- " + p.getKey() + " :: " + p.getValue());
    }


    //Used when recovering, applies the received operations to the database
    public BankImpl(DataAccess dataAccess, List<BankOperation> operations) {
        database = dataAccess;
        for(BankOperation operation: operations)
            database.recoverOperation(operation);
    }

    //Used when not recovering
    public BankImpl(DataAccess dataAccess) {
        database = dataAccess;

    }

    @Override
    public String create(int amount) {
        return Integer.toString(database.insertNewAccount(0));
    }

    @Override
    public Integer balance(String account) {
        return database.getClientBalance(account);
    }

    @Override
    public boolean movement(String account, int amount) {
        Integer balance = database.getClientBalance(Integer.parseInt(account));

        if(balance == null || (amount < 0 && amount + balance < 0))
            return false;

        database.makeMovement(Integer.parseInt(account), amount);
        return true;
    }

    @Override
    public boolean transfer(String origin, String destination, int amount) {
        Integer balanceFrom = database.getClientBalance(origin);

        if(balanceFrom == null || (amount < 0 && amount + balanceFrom < 0))
            return false;


        return makeTransfer(Integer.parseInt(origin), Integer.parseInt(destination), amount);
    }
}
