package bank;

/**
 * Created by joaorodrigues on 18 Apr 16.
 */
public class MovementOperation extends BankOperation {
    private int amount;
    private int finalBalance;
    private String account;

    public MovementOperation(int id, int amount, int finalBalance, String account) {
        super(id);
        this.amount = amount;
        this.finalBalance = finalBalance;
        this.account = account;
    }

    public int getAmount() {
        return amount;
    }

    public int getFinalBalance() {
        return finalBalance;
    }

    public String getAccount() {
        return account;
    }
}
