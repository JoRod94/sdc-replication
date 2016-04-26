package bank;

import data.DataAccess;

import java.io.Serializable;
import java.util.*;
/**
 * Created by joaorodrigues on 14 Apr 16.
 */
public class BankImpl implements Bank, Serializable {
    private DataAccess database;

    /**
     * Recovery mode constructor.
     * Used when the current server is recovering to a new state
     * @param dataAccess - database access layer to be used by the object
     * @param operations - pending operations to process
     */
    public BankImpl(DataAccess dataAccess, List<BankOperation> operations) {
        database = dataAccess;
        doRecovery(operations);
    }

    /**
     * Normal mode constructor.
     * @param dataAccess - database access layer to be used by the object
     */
    public BankImpl(DataAccess dataAccess) {
        database = dataAccess;

    }

    /**
     * Apply a list of pending operations
     * @param op_list - pending operations to be applied
     */
    private void doRecovery(List<BankOperation> op_list){
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

    /**
     * Recovery mode helper. Applies a CREATE operation
     * @param recovered_accounts - accounts that have already bin recovered
     * @param co - operation to be applied
     */
    private void recoverCreateAccountOperation(Set<String> recovered_accounts, BankOperation.Create co){
        if(!recovered_accounts.contains(co.getAccount())){
            recovered_accounts.add(co.getAccount());
            database.makeNewAccount(Integer.parseInt(co.getAccount()), 0, true);
        }
        database.logNewAccount(co.getId(), Integer.parseInt(co.getAccount()), 0, true);
    }

    /**
     * Recovery mode helper. Applies a MOVEMENT operation
     * @param recovered_accounts - accounts that have already bin recovered
     * @param mo - operation to be applied
     */
    private void recoverMovementOperation(Set<String> recovered_accounts, BankOperation.Movement mo){
        if(!recovered_accounts.contains(mo.getAccount())){
            if(!database.hasAccount(Integer.parseInt(mo.getAccount())))
                database.makeNewAccount(Integer.parseInt(mo.getAccount()), mo.getFinalBalance(), true);
            else
                database.updateBalance(Integer.parseInt(mo.getAccount()), mo.getFinalBalance());
            recovered_accounts.add(mo.getAccount());
        }
        database.makeMovement(mo.getId(), mo.getAmount(), Integer.parseInt(mo.getAccount()), mo.getFinalBalance(), true);
    }

    /**
     * Recovery mode helper. Applies a TRANSFER operation
     * @param recovered_accounts - accounts that have already bin recovered
     * @param to - operation to be applied
     */
    private void recoverTransferOperation(Set<String> recovered_accounts, BankOperation.Transfer to){
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

        if(balanceFrom == null || balanceTo == null || balanceFrom - amount < 0 || amount < 0)
            return false;

        database.makeTransfer(0, amount, Integer.parseInt(origin), Integer.parseInt(destination), balanceFrom-amount,
                balanceTo+amount, false);
        return true;
    }

    @Override
    public String latest(String account, int n) {
        if(database.hasAccount(Integer.parseInt(account)))
            return database.getLastAccountOperations(Integer.parseInt(account), n);
        else return null;
    }
}
