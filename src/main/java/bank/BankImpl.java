package bank;

import data.DataAccess;

import java.io.Serializable;
import java.util.ArrayList;
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

    public BankImpl(BankImpl b) {
        System.out.println("RECOVERING STATE...");
        accounts = b.accounts;
        id = b.id;
        for(Map.Entry<String, Integer> p : accounts.entrySet())
            System.out.println("- " + p.getKey() + " :: " + p.getValue());
    }


    //Used when recovering
    public BankImpl(DataAccess dataAccess, ArrayList<BankOperation> operations) {
        database = dataAccess;
        for(BankOperation operation: operations)
            database.insertOperation(operation);
        //Aplicar todas as transaçoes, estas sao a partir do ID mais recente.
    }

    //Used when not recovering
    public BankImpl(DataAccess dataAccess) {
        database = dataAccess;

    }

    @Override
    public String create(int amount) {
        String accountId = Integer.toString(++id);
        accounts.put(accountId, amount);
        return accountId;
    }

    @Override
    public Integer balance(String account) {
        return accounts.get(account);
    }

    @Override
    public boolean movement(String account, int amount) {
        Integer balance = accounts.get(account);

        if(balance == null || (amount < 0 && amount + balance < 0))
            return false;

        accounts.put(account, amount + balance);
        return true;
    }

    @Override
    public boolean transfer(String origin, String destination, int amount) {
        return movement(origin, amount) && movement(destination, amount);
    }
}
