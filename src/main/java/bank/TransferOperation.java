package bank;

/**
 * Created by joaorodrigues on 18 Apr 16.
 */
public class TransferOperation extends BankOperation{
    private int amount;
    private String accountTo;
    private int finalBalanceFrom;
    private int finalBalanceTo;

    public TransferOperation(int id, int amount, String accountFrom, String accountTo, int finalBalanceFrom, int finalBalanceTo) {
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
