package bank;

import client.BankStub;
import data.DataAccess;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import java.util.*;
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
        doRecovery(operations);
    }

    //Used when not recovering
    public BankImpl(DataAccess dataAccess) {
        database = dataAccess;

    }

    public void doRecovery(List<BankOperation> op_list){
        System.out.println(op_list);

        Set<String> recovered_accounts = new HashSet<>();

        for(BankOperation operation : op_list){
            if(operation instanceof BankOperation.Create){
                recoverCreateAccountOperation(recovered_accounts, (BankOperation.Create) operation);
            } else if(operation instanceof BankOperation.Movement) {
                recoverMovementOperation(recovered_accounts, (BankOperation.Movement) operation);
            } else if(operation instanceof BankOperation.Transfer) {
                recoverTransferOperation(recovered_accounts, (BankOperation.Transfer) operation);
            }
        }

        database.refreshCurrentAccountId();
        database.refreshCurrentOperationId();
    }

    public void recoverCreateAccountOperation(Set<String> recovered_accounts, BankOperation.Create co){
        if(!recovered_accounts.contains(co.getAccount())){
            recovered_accounts.add(co.getAccount());
            database.makeNewAccount(Integer.parseInt(co.getAccount()), 0, true);
        }
        database.logNewAccount(Integer.parseInt(co.getAccount()), 0);
    }

    public void recoverMovementOperation(Set<String> recovered_accounts, BankOperation.Movement mo){
        if(!recovered_accounts.contains(mo.getAccount())){
            if(!database.hasAccount(Integer.parseInt(mo.getAccount())))
                database.makeNewAccount(Integer.parseInt(mo.getAccount()), mo.getFinalBalance(), true);
            else
                database.updateBalance(Integer.parseInt(mo.getAccount()), mo.getFinalBalance());
            recovered_accounts.add(mo.getAccount());
        }
        database.makeMovement(mo.getId(), mo.getAmount(), Integer.parseInt(mo.getAccount()), mo.getFinalBalance(), true);
    }

    public void recoverTransferOperation(Set<String> recovered_accounts, BankOperation.Transfer to){
        boolean from_recovered = recovered_accounts.contains(to.getAccountFrom());
        boolean to_recovered = recovered_accounts.contains(to.getAccountTo());

        if(!from_recovered){
            if(!database.hasAccount(Integer.parseInt(to.getAccountFrom())))
                database.makeNewAccount(Integer.parseInt(to.getAccountFrom()), to.getFinalBalanceFrom(), true);
            else
                database.updateBalance(Integer.parseInt(to.getAccountFrom()), to.getFinalBalanceFrom());
            recovered_accounts.add(to.getAccountFrom());
        }
        if(!to_recovered) {
            if(!database.hasAccount(Integer.parseInt(to.getAccountTo())))
                database.makeNewAccount(Integer.parseInt(to.getAccountTo()), to.getFinalBalanceTo(), true);
            else
                database.updateBalance(Integer.parseInt(to.getAccountTo()), to.getFinalBalanceTo());
            recovered_accounts.add(to.getAccountTo());
        }

        database.makeTransfer(to.getId(), to.getAmount(), Integer.parseInt(to.getAccountFrom()),
                Integer.parseInt(to.getAccountTo()), to.getFinalBalanceFrom(), to.getFinalBalanceTo(), true);
    }

    @Override
    public String create() {
        return Integer.toString(database.makeNewAccount(0, 0,false));
    }

    @Override
    public Integer balance(String account) {
        return database.getAccountBalance(Integer.parseInt(account));
    }

    @Override
    public boolean movement(String account, int amount) {
        Integer balance = database.getAccountBalance(Integer.parseInt(account));

        if(balance == null || (amount < 0 && (amount + balance < 0)))
            return false;

        database.makeMovement(0, amount, Integer.parseInt(account), amount+balance, false);
        return true;
    }

    @Override
    public boolean transfer(String origin, String destination, int amount) {
        Integer balanceFrom = database.getAccountBalance(Integer.parseInt(origin));
        Integer balanceTo = database.getAccountBalance(Integer.parseInt(destination));

        if(balanceFrom == null || balanceTo == null || (amount < 0 && balanceFrom - amount < 0) || amount < 0)
            return false;

        database.makeTransfer(0, amount, Integer.parseInt(origin), Integer.parseInt(destination), balanceFrom-amount,
                balanceTo+amount, false);
        return true;
    }
}
