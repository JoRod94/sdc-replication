package bank;

/**
 * Created by joaorodrigues on 18 Apr 16.
 */
public abstract class BankOperation {
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

}
