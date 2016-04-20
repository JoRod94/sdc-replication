package bank;

/**
 * Created by joaorodrigues on 18 Apr 16.
 */
public class MovementOperation extends BankOperation {
    private int amount;
    private int finalBalance;

    public MovementOperation(int id, int amount, int finalBalance, String account) {
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
