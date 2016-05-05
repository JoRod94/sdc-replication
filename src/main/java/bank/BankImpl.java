package bank;

import data.DataAccess;

import java.io.Serializable;
import java.sql.Connection;
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
     * Apply a list of pending operations (executed in recovery mode). If recovery fails (due to database exception), it stops.
     * @param op_list - pending operations to be applied
     */
    private void doRecovery(List<BankOperation> op_list){
        boolean stmt_success = true;

        //Stores already recovered accounts. The operation list is recovered backwards to avoid re-writing
        //data related to the same object (account)
        Set<String> recovered_accounts = new HashSet<>();

        //Creates a recovery connection where the global transaction will be made (in order to avoid empty spaces in the
        //log table in case the recovery fails)
        Connection con = database.getTransactionConnection();
        database.initTransaction(con);

        //Recovers each operation individually
        for(BankOperation operation : op_list){
            if(operation instanceof BankOperation.Create){
                stmt_success = recoverCreateAccountOperation(recovered_accounts, (BankOperation.Create) operation, con);
            } else if(operation instanceof BankOperation.Movement) {
                stmt_success = recoverMovementOperation(recovered_accounts, (BankOperation.Movement) operation, con);
            } else if(operation instanceof BankOperation.Transfer) {
                stmt_success = recoverTransferOperation(recovered_accounts, (BankOperation.Transfer) operation, con);
            }

            if(!stmt_success){
                System.out.print("Recovery failed");
                break;
            }
        }

        //The transaction is committed in case it was successful
        database.commitTransaction(con);

        //The database table counters are refreshed
        database.refreshCurrentAccountId();
        database.refreshCurrentOperationId();
    }

    /**
     * Recovery mode helper. Applies a CREATE operation
     * @param recovered_accounts - accounts that have already been recovered
     * @param co - operation to be applied
     * @param con - connection to be used for the transaction
     */
    private boolean recoverCreateAccountOperation(Set<String> recovered_accounts, BankOperation.Create co, Connection con){
        boolean recover = true;

        //Only recovers (creates) the account if it wasn't already recovered
        if(!recovered_accounts.contains(co.getAccount())){
            recovered_accounts.add(co.getAccount());
            recover = recover && database.recoverAccount(Integer.parseInt(co.getAccount()), 0, con);
        }

        //Logs the create operation with the given id
        return recover && database.logNewAccount(co.getId(), Integer.parseInt(co.getAccount()), 0, con);
    }

    /**
     * Recovery mode helper. Applies a MOVEMENT operation
     * @param recovered_accounts - accounts that have already been recovered
     * @param mo - operation to be applied
     * @param con - connection to be used for the transaction
     */
    private boolean recoverMovementOperation(Set<String> recovered_accounts, BankOperation.Movement mo, Connection con){
        boolean recover = true;

        //Only recovers (creates) the account if it wasn't already recovered
        if(!recovered_accounts.contains(mo.getAccount())){
            if(!database.hasAccount(Integer.parseInt(mo.getAccount())))
                //If the account doesn't exist, creates it with its current balance
                recover = recover && database.recoverAccount(Integer.parseInt(mo.getAccount()), mo.getFinalBalance(), con);
            else
                //If the account already exists, updates it's balance
                recover = recover && database.updateBalance(Integer.parseInt(mo.getAccount()), mo.getFinalBalance(), con);
            recovered_accounts.add(mo.getAccount());
        }

        //Logs the movement operation
        return recover && database.recoverMovement(mo.getId(), mo.getAmount(), Integer.parseInt(mo.getAccount()), mo.getFinalBalance(), con);
    }

    /**
     * Recovery mode helper. Applies a TRANSFER operation
     * @param recovered_accounts - accounts that have already been recovered
     * @param to - operation to be applied
     * @param con - connection to be used for the transaction
     */
    private boolean recoverTransferOperation(Set<String> recovered_accounts, BankOperation.Transfer to, Connection con){
        boolean recover = true;
        boolean from_recovered = recovered_accounts.contains(to.getAccountFrom());
        boolean to_recovered = recovered_accounts.contains(to.getAccountTo());

        //For each transfer participating account, recovers using the same logic used in the recoverMovementOperation
        if(!from_recovered){
            if(!database.hasAccount(Integer.parseInt(to.getAccountFrom())))
                recover = recover && database.recoverAccount(Integer.parseInt(to.getAccountFrom()), to.getFinalBalanceFrom(), con);
            else
                recover = recover && database.updateBalance(Integer.parseInt(to.getAccountFrom()), to.getFinalBalanceFrom(), con);
            recovered_accounts.add(to.getAccountFrom());
        }
        if(!to_recovered) {
            if(!database.hasAccount(Integer.parseInt(to.getAccountTo())))
                recover = recover && database.recoverAccount(Integer.parseInt(to.getAccountTo()), to.getFinalBalanceTo(), con);
            else
                recover = recover && database.updateBalance(Integer.parseInt(to.getAccountTo()), to.getFinalBalanceTo(), con);
            recovered_accounts.add(to.getAccountTo());
        }

        //Logs the transfer operation
        return recover && database.recoverTransfer(to.getId(), to.getAmount(), Integer.parseInt(to.getAccountFrom()),
                Integer.parseInt(to.getAccountTo()), to.getFinalBalanceFrom(), to.getFinalBalanceTo(), con);
    }

    @Override
    public String create() {
        return Integer.toString(database.makeNewAccount(0));
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

        database.makeMovement(amount, Integer.parseInt(account), amount+balance);
        return true;
    }

    @Override
    public boolean transfer(String origin, String destination, int amount) {
        Integer balanceFrom = database.getAccountBalance(Integer.parseInt(origin));
        Integer balanceTo = database.getAccountBalance(Integer.parseInt(destination));

        if(balanceFrom == null || balanceTo == null || balanceFrom - amount < 0 || amount < 0)
            return false;

        database.makeTransfer(amount, Integer.parseInt(origin), Integer.parseInt(destination), balanceFrom-amount,
                balanceTo+amount);

        return true;
    }

    @Override
    public String latest(String account, int n) {
        return database.hasAccount(Integer.parseInt(account))
                ? database.getLastAccountOperations(Integer.parseInt(account), n)
                : null;
    }
}
