package bank;

/**
 * Created by joaorodrigues on 18 Apr 16.
 */
public abstract class BankOperation {
    private int id;

    public BankOperation(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }


}
