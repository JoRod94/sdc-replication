package bank;

import java.io.Serializable;

/**
 * Created by joaorodrigues on 18 Apr 16.
 */
public abstract class BankOperation implements Serializable{
    private int id;
    private String account;

    public BankOperation(int id, String account) {
        this.id = id;
        this.account = account;
    }

    public int getId() {
        return id;
    }

    public String getAccount() { return account; }

    public static class Create extends BankOperation {
        public Create(int id, String account) {
            super(id, account);
        }

    }

    public static class Movement extends BankOperation {
        private int amount;
        private int finalBalance;

        public Movement(int id, int amount, int finalBalance, String account) {
            super(id, account);
            this.amount = amount;
            this.finalBalance = finalBalance;
        }

        public int getAmount() {
            return amount;
        }

        public int getFinalBalance() {
            return finalBalance;
        }

    }

    public static class Transfer extends BankOperation {
        private int amount;
        private String accountTo;
        private int finalBalanceFrom;
        private int finalBalanceTo;

        public Transfer(int id, int amount, String accountFrom, String accountTo, int finalBalanceFrom, int finalBalanceTo) {
            super(id, accountFrom);
            this.amount = amount;
            this.accountTo = accountTo;
            this.finalBalanceFrom = finalBalanceFrom;
            this.finalBalanceTo = finalBalanceTo;
        }

        public int getAmount() {
            return amount;
        }

        public String getAccountFrom() {
            return this.getAccount();
        }

        public String getAccountTo() {
            return accountTo;
        }

        public int getFinalBalanceFrom() {
            return finalBalanceFrom;
        }

        public int getFinalBalanceTo() {
            return finalBalanceTo;
        }
    }

}
