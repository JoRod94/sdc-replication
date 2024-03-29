package bank;

import data.Cacheable;

/**
 * Created by frm on 18/04/16.
 * This class should be used as a CacheObject entry.
 * It's a simple container to be able to keep
 * a simple association between an account and it's current balance.
 */
public class Account implements Cacheable {
    int id;
    int balance;

    public Account(int id, int balance) {
        this.balance = balance;
        this.id = id;
    }

    @Override
    public int getId() {
        return id;
    }

    /**
     * Sets the account id
     * @param id
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Returns the account ballance
     * @return
     */
    public int getBalance() {
        return balance;
    }

    /**
     * Sets the new account balance
     * @param balance
     */
    public void setBalance(int balance) {
        this.balance = balance;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
