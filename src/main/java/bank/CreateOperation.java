package bank;

/**
 * Created by joaorodrigues on 18 Apr 16.
 */
public class CreateOperation extends BankOperation {
    private String account;


    public CreateOperation(int id, String account) {
        super(id);
        this.account = account;
    }

    public String getAccount() {
        return account;
    }
}
