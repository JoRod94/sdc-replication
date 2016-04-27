package bank;

import java.io.Serializable;

/**
 * Created by joaorodrigues on 18 Apr 16.
 * BankOperation is an abstract class that should serve as a behavioural wrapper for its subclasses.
 * It is used in recovery mode, since a server sends a list of BankOperations to the ones that request it.
 */
public abstract class BankOperation implements Serializable{
    private int id;
    private String account;

    /**
     * Creates a BankOperation bound to a given account.
     * The BankOperation is identified by a single, unique id.
     * @param id - unique id identifying the operation.
     * @param account - unique account id that the object will be bound to.
     */
    public BankOperation(int id, String account) {
        this.id = id;
        this.account = account;
    }

    /**
     * Returns the operation unique id
     * @return
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the account id the operation is bound to
     * @return
     */
    public String getAccount() { return account; }

    /**
     * First of a subset of classes that specify the behaviour and type of a BankOperation.
     * Identifies a CREATE operation.
     * No extra fields are required by this operation type.
     */
    public static class Create extends BankOperation {
        /**
         * Creates a BankOperation representing a CREATE operation
         * @param id - operation unique id
         * @param account - account unique id
         */
        public Create(int id, String account) {
            super(id, account);
        }
    }

    /**
     * Class that specifies the behaviour and type of a BankOperation.
     * Identifies a MOVEMENT operation.
     * This account type required extra fields identifying the final balance and the movement amount.
     */
    public static class Movement extends BankOperation {
        private int amount;
        private int finalBalance;

        /**
         * Creates a BankOperation representing a MOVEMENT operation
         * @param id - operation unique id
         * @param amount - amount moved into or from the account
         * @param finalBalance - account balance after the operation
         * @param account - account unique id
         */
        public Movement(int id, int amount, int finalBalance, String account) {
            super(id, account);
            this.amount = amount;
            this.finalBalance = finalBalance;
        }

        /**
         * @return movement amount
         */
        public int getAmount() {
            return amount;
        }

        /**
         * @return final balance the account had after this operation.
         */
        public int getFinalBalance() {
            return finalBalance;
        }

    }

    /**
     * Class that specifies the behaviour and type of a BankOperation.
     * Identifies a TRANSFER operation.
     * This account type required extra fields identifying the destination account, final balance of both accounts and the movement amount.
     */
    public static class Transfer extends BankOperation {
        private int amount;
        private String accountTo;
        private int finalBalanceFrom;
        private int finalBalanceTo;

        /**
         * Creates a BankOperation representing a TRANSFER operation
         * @param id - operation unique id
         * @param amount - amount moved into or from the account
         * @param accountFrom - origin account unique id
         * @param accountTo - destination account unique id
         * @param finalBalanceFrom - origin account final balance
         * @param finalBalanceTo - destination account final balance
         */
        public Transfer(int id, int amount, String accountFrom, String accountTo, int finalBalanceFrom, int finalBalanceTo) {
            super(id, accountFrom);
            this.amount = amount;
            this.accountTo = accountTo;
            this.finalBalanceFrom = finalBalanceFrom;
            this.finalBalanceTo = finalBalanceTo;
        }

        /**
         * @return - transfer amount
         */
        public int getAmount() {
            return amount;
        }

        /**
         * @return - origin account unique id
         */
        public String getAccountFrom() {
            return this.getAccount();
        }

        /**
         * @return - destination account unique id
         */
        public String getAccountTo() {
            return accountTo;
        }

        /**
         * @return - origin account final balance
         */
        public int getFinalBalanceFrom() {
            return finalBalanceFrom;
        }

        /**
         * @return - destination account final balance
         */
        public int getFinalBalanceTo() {
            return finalBalanceTo;
        }
    }

}
