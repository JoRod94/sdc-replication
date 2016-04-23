package bank;

/**
 * Created by frm on 16/04/16.
 */
public interface Bank {
    /**
     * Creates a new account
     * @return - account unique ID
     */
    String create();

    /**
     * Access the account balance
     * @param account - account unique ID
     * @return - account balance. Will be null if the account doesn't exist
     */
    Integer balance(String account);

    /**
     * Makes a movement in a given account.
     * If the amount is positive, it will be considered a deposit
     * and the amount will be added.
     * If the amount is negative, it will be considered a withdrawal.
     * In this case it will only be processed if the account has enough balance
     * @param account - unique account ID to process the movement
     * @param amount - amount to be withdrawn/deposited
     * @return - boolean indicating if the movement was successful
     */
    boolean movement(String account, int amount);

    /**
     * Transfers a given amount of money between two accounts.
     * This method will only process the transfer if the origin account
     * has enough balance.
     * @param origin - account from where to transfer the money from
     * @param destination - account to where the money will go
     * @param amount - transfer amount
     * @return - indicates if it was possible to transfer, or not, the account
     */
    boolean transfer(String origin, String destination, int amount);

    /**
     * Gets the last n movements made by a given account
     * @param account - account to check
     * @param n - number of movements
     * @return - list of movements
     */
    // TODO
    // List<Movement> movements(String account, int n);
}
