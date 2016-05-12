package data;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import bank.*;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class DataAccess {
    EmbeddedDataSource rawDataSource;

    private static final String DB_PATH = "./src/main/resources/db";
    private static final String DB_FILENAME = "BankData";

    public enum OP_TYPES {MOVEMENT, TRANSFER, CREATE};
    private int currentAccountId, currentOperationId;
    private static ReentrantLock accountLock = new ReentrantLock();
    private static ReentrantLock operationLock = new ReentrantLock();
    private CacheManager<Account> cache;

    /**
     * Initiates database connection, creating it if it doesn't exist already.
     * @param name database folder name
     * @throws SQLException
     */
    public void initEDBConnection(String name) throws SQLException {
        String dbName = buildDBName(name);
        cache = new CacheManager<>(1024);
        File f = new File(dbName);

        if (!f.exists())
            createDB(dbName);
        else if(!f.isDirectory())
            createDB(dbName);
        else
            connectDB(dbName);

        refreshCurrentAccountId();
        refreshCurrentOperationId();
    }

    private void createTables() throws SQLException {
        createAccountsTable();
        createOperationTypeTable();
        createOperationsTable();
    }

    private void dropTables() throws SQLException {
        dropTable("OPERATIONS");
        dropTable("ACCOUNTS");
        dropTable("OPERATION_TYPE");
    }

    private void connectTo(String dbName, boolean create) {
        rawDataSource = new EmbeddedDataSource();
        rawDataSource.setDatabaseName(dbName);
        if(create)
            rawDataSource.setCreateDatabase("create");
    }

    private void connectDB(String dbName) {
        connectTo(dbName, false);
    }

    private void createDB(String dbName) throws SQLException {
        connectTo(dbName, true);
        createTables();
    }

    /**
     * Executes an update query in the database
     * @param query query to be executed
     */
    public void dbUpdate(String query) {
        try {
            tryDbUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Executes an update query in the database
     * @param query query to be executed
     * @param con connection to be used
     */
    public void dbUpdate(String query, Connection con) {
        try {
            tryDbUpdate(query, con);
        } catch (SQLException e) {
            try {
                con.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    /**
     * Returns a connection to the instantiated database
     * @return database connection
     */
    public Connection getTransactionConnection(){
        try {
            return rawDataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Prepares the connection to execute a transaction
     * @param con connection to be used
     */
    public void initTransaction(Connection con) {
        try {
            con.setAutoCommit(false);
            con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        } catch (SQLException e) {
            try {
                con.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    /**
     * Commits a transaction being executed withing the given connection
     * @param con connection to be used
     */
    public void commitTransaction(Connection con){
        try {
            con.commit();
            con.setAutoCommit(true);
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        } catch (SQLException e) {
            try {
                con.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

    /**
     * Executes an update query in the database (throws SQLException)
     * @param query query to be executed
     */
    private void tryDbUpdate(String query) throws SQLException {
        Statement s = null;
        try {
            s = rawDataSource.getConnection().createStatement();
            s.executeUpdate(query);
        } finally {
            s.close();
        }
    }

    /**
     * Executes an update query in the database (throws SQLException)
     * @param query query to be executed
     * @param con database connection to be used (used in the recovery transaction context)
     * @throws SQLException
     */
    private void tryDbUpdate(String query, Connection con) throws SQLException {
        Statement s = null;
        try {
            s = con.createStatement();
            s.executeUpdate(query);
        } finally {
            s.close();
        }
    }

    public void createAccountsTable() throws SQLException {
        tryDbUpdate("create table ACCOUNTS ("
                + "ACCOUNT_ID INTEGER PRIMARY KEY, "
                + "BALANCE INTEGER,"
                + "TIMESTAMP TIMESTAMP)");
    }

    public void createOperationsTable() throws SQLException {
        tryDbUpdate("create table OPERATIONS ("
                + "OP_ID INTEGER PRIMARY KEY, "
                + "OP_TYPE INTEGER NOT NULL, "
                + "MV_AMOUNT INTEGER, "
                + "FROM_ACCOUNT_ID INTEGER, "
                + "TO_ACCOUNT_ID INTEGER, "
                + "FROM_CURRENT_BALANCE INTEGER, "
                + "TO_CURRENT_BALANCE INTEGER, "
                + "TIMESTAMP TIMESTAMP, "
                + "CONSTRAINT OP_TYPE_REF FOREIGN KEY (OP_TYPE) REFERENCES OPERATION_TYPE(OP_TYPE), "
                + "CONSTRAINT FROM_ACCOUNT_ID_REF FOREIGN KEY (FROM_ACCOUNT_ID) REFERENCES ACCOUNTS(ACCOUNT_ID), "
                + "CONSTRAINT TO_ACCOUNT_ID_REF FOREIGN KEY (TO_ACCOUNT_ID) REFERENCES ACCOUNTS(ACCOUNT_ID))");
    }

    public void createOperationTypeTable() throws SQLException {
        tryDbUpdate("create table OPERATION_TYPE ("
                + "OP_TYPE INTEGER PRIMARY KEY, "
                + "OP_DESIGNATION VARCHAR(20))");

        //populates de operation type table with the existing types
        populateOperationType();
    }

    public void populateOperationType(){
        for(OP_TYPES ot : OP_TYPES.values())
            dbUpdate("insert into OPERATION_TYPE (OP_TYPE, OP_DESIGNATION) values " +
                    "("+(ot.ordinal()+1)+",\'"+ot.name()+"\')");
    }

    /**
     * Drop table
     * @param tablename name of the table to be dropped
     * @throws SQLException
     */
    public void dropTable(String tablename) throws SQLException {
        tryDbUpdate("DROP TABLE " + tablename);
    }

    /**
     * Make movement in normal execution mode (logs the movement - operations table - and updates the account balance - accounts table)
     * @param mv_amount movement amount
     * @param account_id account where the movement will be executed
     * @param final_balance new account balance to be updated
     * @return inserted operation id
     */
    public int makeMovement(int mv_amount, int account_id, int final_balance){
        int generated_id = 0;

        try {
            operationLock.lock();
            executeMovement(generated_id = currentOperationId++, mv_amount, account_id, final_balance, rawDataSource.getConnection());
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            operationLock.unlock();
        }

        updateBalance(account_id, final_balance);

        return generated_id;
    }

    /**
     * Make movement in recovery execution mode(logs de movement with given id - operations table)
     * @param op_id operation id to be inserted in the log
     * @param mv_amount movement amount
     * @param account_id account where the movement will be executed
     * @param final_balance account balance at the execution of the movement
     * @param con database connection to be used
     */
    public boolean recoverMovement(int op_id, int mv_amount, int account_id, int final_balance, Connection con){
        try {
            executeMovement(op_id, mv_amount, account_id, final_balance, con);
        } catch (SQLException e) {
            try {
                con.rollback();
                return false;
            } catch (SQLException e1) {
                e1.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * Execute movement statement with given parameters
     * @param op_id logged operation id
     * @param mv_amount logged movement amount
     * @param account_id logged account_id
     * @param final_balance logged final balance
     * @param con connection to be used
     * @throws SQLException
     */
    public void executeMovement(int op_id, int mv_amount, int account_id, int final_balance, Connection con) throws SQLException {
        PreparedStatement stmt = con.prepareStatement(
                "insert into OPERATIONS (OP_ID, OP_TYPE, MV_AMOUNT, FROM_ACCOUNT_ID, FROM_CURRENT_BALANCE, TIMESTAMP) " +
                        "values (?,?,?,?,?,?)");

        stmt.setInt(1, op_id);
        stmt.setInt(2, OP_TYPES.valueOf("MOVEMENT").ordinal()+1);
        stmt.setInt(3, mv_amount);
        stmt.setInt(4, account_id);
        stmt.setInt(5, final_balance);
        stmt.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
        stmt.execute();
        stmt.close();
    }

    /**
     * Make transfer in normal execution mode (logs the transfer - operations table - and updates the accounts balance - accounts table)
     * @param tr_amount transfer amount
     * @param from_account account id to withdraw
     * @param to_account account id to deposit the money
     * @param from_final_balance withdrawn account final balance
     * @param to_final_balance deposited account final balance
     * @return inserted operation id
     */
    public int makeTransfer(int tr_amount, int from_account, int to_account, int from_final_balance, int to_final_balance) {
        int generated_id = 0;

        try {
            operationLock.lock();
            executeTransfer(generated_id = currentOperationId++, tr_amount, from_account,
                    to_account, from_final_balance, to_final_balance, rawDataSource.getConnection());
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            operationLock.unlock();
        }

        updateBalance(from_account, from_final_balance);
        updateBalance(to_account, to_final_balance);

        return generated_id;
    }

    /**
     * Make transfer in recovery execution mode(logs de transfer with given id - operations table)
     * @param op_id operation id to be inserted in the log
     * @param tr_amount transfer amount
     * @param from_account account id where the money was withdrawn
     * @param to_account account id where the money was deposited
     * @param from_final_balance withdrawn account final balance
     * @param to_final_balance deposited account final balance
     * @param con connection to be used
     */
    public boolean recoverTransfer(int op_id, int tr_amount, int from_account, int to_account, int from_final_balance,
                               int to_final_balance, Connection con){
        try {
            executeTransfer(op_id, tr_amount, from_account, to_account, from_final_balance, to_final_balance, con);
        } catch (SQLException e) {
            try {
                con.rollback();
                return false;
            } catch (SQLException e1) {
                e1.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * Execute transfer statement with given parameters
     * @param op_id logged operation id
     * @param tr_amount logged transfer amount
     * @param from_account logged withdrawn account
     * @param to_account logger deposited account
     * @param from_final_balance logged withdrawn account final balance
     * @param to_final_balance logged deposited account final balance
     * @param con connection to be used
     * @throws SQLException
     */
    public void executeTransfer(int op_id, int tr_amount, int from_account, int to_account, int from_final_balance,
                                int to_final_balance, Connection con) throws SQLException {

        PreparedStatement stmt = con.prepareStatement(
                "insert into OPERATIONS (OP_ID, OP_TYPE, MV_AMOUNT, FROM_ACCOUNT_ID, TO_ACCOUNT_ID, FROM_CURRENT_BALANCE, " +
                        "TO_CURRENT_BALANCE, TIMESTAMP) values (?,?,?,?,?,?,?,?)");

        stmt.setInt(1, op_id);
        stmt.setInt(2, OP_TYPES.valueOf("TRANSFER").ordinal()+1);
        stmt.setInt(3, tr_amount);
        stmt.setInt(4, from_account);
        stmt.setInt(5, to_account);
        stmt.setInt(6, from_final_balance);
        stmt.setInt(7, to_final_balance);
        stmt.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
        stmt.execute();
        stmt.close();
    }

    /**
     * Log account creation in the operations table (recovery mode)
     * @param op_id logged operation id
     * @param account_id logged account id
     * @param current_balance logged balance
     * @param con connection to be used
     */
    public boolean logNewAccount(int op_id, int account_id, int current_balance, Connection con){
        try {
            PreparedStatement stmt = con.prepareStatement(
                    "insert into OPERATIONS (OP_ID, OP_TYPE, FROM_ACCOUNT_ID, FROM_CURRENT_BALANCE, TIMESTAMP) " +
                            "values (?,?,?,?,?)");
            stmt.setInt(1, op_id);
            stmt.setInt(2, OP_TYPES.valueOf("CREATE").ordinal()+1);
            stmt.setInt(3, account_id);
            stmt.setInt(4, current_balance);
            stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            stmt.close();
        } catch (SQLException e) {
            try {
                con.rollback();
                return false;
            } catch (SQLException e1) {
                e1.printStackTrace();
                return false;
            }
        }

        return true;
    }

    /**
     * Log account creation in the operations table
     * @param op_id logged operation id
     * @param account_id logged account id
     * @param current_balance logged balance
     */
    public void logNewAccount(int op_id, int account_id, int current_balance){
        try {
            PreparedStatement stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into OPERATIONS (OP_ID, OP_TYPE, FROM_ACCOUNT_ID, FROM_CURRENT_BALANCE, TIMESTAMP) " +
                            "values (?,?,?,?,?)");
            stmt.setInt(1, op_id);
            stmt.setInt(2, OP_TYPES.valueOf("CREATE").ordinal()+1);
            stmt.setInt(3, account_id);
            stmt.setInt(4, current_balance);
            stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Make new account in recovery execution mode(logs de creation with given id - accounts table)
     * @param account_id
     * @param balance
     * @param con
     */
    public boolean recoverAccount(int account_id, int balance, Connection con){
        try {
            executeNewAccount(account_id, balance, con);
        } catch (SQLException e) {
            try {
                con.rollback();
                return false;
            } catch (SQLException e1) {
                e1.printStackTrace();
                return false;
            }
        }

        cache.add(new Account(account_id, balance));
        return true;
    }

    /**
     * Make new account in normal execution mode(logs the new account - accounts table - and logs the operation - operations table)
     * @param balance initial account balance
     * @return generated account id
     */
    public int makeNewAccount(int balance){
        int generated_id = 0;

        try {
            accountLock.lock();
            executeNewAccount(generated_id = currentAccountId++, balance, rawDataSource.getConnection());
            accountLock.unlock();

            cache.add(new Account(generated_id, balance));

            operationLock.lock();
            logNewAccount(currentOperationId, generated_id, balance);
            currentOperationId++;
            operationLock.unlock();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return generated_id;
    }

    /**
     * Execute new account creation with given parameters
     * @param account_id logged account id
     * @param balance logged initial account balance
     * @param con connection to be used
     * @throws SQLException
     */
    public void executeNewAccount(int account_id, int balance, Connection con) throws SQLException {
        PreparedStatement stmt = con.prepareStatement(
                "insert into ACCOUNTS (ACCOUNT_ID, BALANCE, TIMESTAMP) values (?,?,?)");

        stmt.setInt(1, account_id);
        stmt.setInt(2, balance);
        stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
        stmt.execute();
        stmt.close();
    }

    /**
     * Update balance in account entry at the accounts table and update balance in the cache
     * @param account_id account id to update
     * @param final_amount amount to update
     */
    public void updateBalance(int account_id, int final_amount){
        dbUpdate("update ACCOUNTS set BALANCE = "+ final_amount + " where ACCOUNT_ID = " + account_id);
        cache.add(new Account(account_id, final_amount));
    }

    /**
     * Update balance in account entry at the accounts table and update balance in the cache (recovery mode)
     * @param account_id account id to update
     * @param final_amount amount to update
     * @param con connection to be used
     */
    public boolean updateBalance(int account_id, int final_amount, Connection con){
        try {
            tryDbUpdate("update ACCOUNTS set BALANCE = "+ final_amount + " where ACCOUNT_ID = " + account_id, con);
            cache.add(new Account(account_id, final_amount));
        } catch (SQLException e) {
            try {
                con.rollback();
                return false;
            } catch (SQLException e1) {
                e1.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * Returns account balance in case the account exists
     * @param account_id account from which to return the balance
     * @return balance of the account, null otherwise
     */
    public Integer getAccountBalance(int account_id){
        Account a = cache.get(account_id);
        if(a != null) return a.getBalance();

        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT BALANCE FROM ACCOUNTS WHERE ACCOUNT_ID = " + account_id)) {

            if (res.next())
                return res.getInt("BALANCE");
        } catch (SQLException ex) {
            return null;
        }

        return null;
    }

    public String getOperationLogs() throws SQLException {
        StringBuilder a = new StringBuilder();
        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT * FROM APP.OPERATIONS")) {
            a.append("List of operation entries: \n");
            while (res.next()) {
                int type = res.getInt("OP_TYPE");
                switch(type){
                    case 1:
                        getMovementLog(a, res);
                        break;
                    case 2:
                        getTransferLog(a, res);
                        break;
                    case 3:
                        getCreateAccountLog(a, res);
                        break;
                }
            }
        }

        return a.toString();
    }

    public void getCreateAccountLog(StringBuilder a, ResultSet res) throws SQLException {
        a.append("Id: " + res.getString("OP_ID"))
                .append("\tType: " + OP_TYPES.values()[res.getInt("OP_TYPE")-1].name())
                .append("\tClient: " + res.getString("FROM_ACCOUNT_ID"))
                .append("\tTimestamp: " + res.getString("TIMESTAMP").toString()+"\n");
    }

    public void getMovementLog(StringBuilder a, ResultSet res) throws SQLException {
        a.append("Id: " + res.getString("OP_ID"))
                .append("\tType: " + OP_TYPES.values()[res.getInt("OP_TYPE")-1].name())
                .append("\tClient: " + res.getString("FROM_ACCOUNT_ID"))
                .append("\tAmount: " + res.getString("MV_AMOUNT"))
                .append("\tBalance: " + res.getString("FROM_CURRENT_BALANCE"))
                .append("\tTimestamp: " + res.getString("TIMESTAMP").toString()+"\n");
    }

    public void getTransferLog(StringBuilder a, ResultSet res) throws SQLException {
        a.append("Id: " + res.getString("OP_ID"))
                .append("\tType: " + OP_TYPES.values()[res.getInt("OP_TYPE")-1].name())
                .append("\tFrom Client: " + res.getString("FROM_ACCOUNT_ID"))
                .append("\tTo Client: " + res.getString("TO_ACCOUNT_ID"))
                .append("\tAmount: " + res.getString("MV_AMOUNT"))
                .append("\tFrom Balance: " + res.getString("FROM_CURRENT_BALANCE"))
                .append("\tTo Balance: " + res.getString("TO_CURRENT_BALANCE"))
                .append("\tTimestamp: " + res.getString("TIMESTAMP").toString()+"\n");
    }

    public String getAccountsInfo(){
        StringBuilder a = new StringBuilder();
        try {
            try (
                    Statement s = rawDataSource.getConnection().createStatement();
                    ResultSet res = s.executeQuery(
                            "SELECT * FROM APP.ACCOUNTS")) {
                a.append("Account list: \n");
                while (res.next()) {
                    a.append("\tID: " + res.getString("ACCOUNT_ID"))
                            .append("\tBalance: " + res.getString("BALANCE"))
                            .append("\tTimestamp: " + res.getString("TIMESTAMP").toString()+"\n");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return a.toString();
    }

    /**
     * Returns textual information about the last n operations on the given account id
     * @param account_id account id associated with the operations
     * @param n max number of operations to return
     * @return string containing information about the last n operations (id, type, amount, balance, timestamp)
     */
    public String getLastAccountOperations(int account_id, int n) {
        StringBuilder a = new StringBuilder();
        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT * FROM OPERATIONS where FROM_ACCOUNT_ID = "+account_id+" ORDER BY TIMESTAMP " +
                                "DESC FETCH FIRST "+n+" ROWS ONLY")) {

            while (res.next()) {
                int type = res.getInt("OP_TYPE");
                switch(type){
                    case 1:
                        getMovementLog(a, res);
                        break;
                    case 2:
                        getTransferLog(a, res);
                        break;
                    case 3:
                        getCreateAccountLog(a, res);
                        break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if(a.length() == 0) a.append("No operations for account "+account_id);
        return a.toString();
    }

    /**
     * Get last account id used
     * @return last account id
     */
    private int getCurrentAccountId() {
        accountLock.lock();
        int nmr = 1;
        try (
                Statement s = rawDataSource.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet res = s.executeQuery(
                        "SELECT ACCOUNT_ID FROM APP.ACCOUNTS ORDER BY ACCOUNT_ID ASC")) {

            if (res.last()) {
                nmr = Integer.parseInt(res.getString("ACCOUNT_ID")) + 1;
            }
        } catch (SQLException ex) {
            return nmr;
        }

        accountLock.unlock();
        return nmr;
    }

    /**
     * Get last operation id used
     * @return last operation id
     */
    public int getCurrentOperationId(){
        int nmr = 1;
        operationLock.lock();
        try (
                Statement s = rawDataSource.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet res = s.executeQuery(
                        "SELECT OP_ID FROM APP.OPERATIONS ORDER BY OP_ID ASC")) {

            if (res.last()) {
                nmr = Integer.parseInt(res.getString("OP_ID")) + 1;
            }
        } catch (SQLException ex) {
            return nmr;
        }

        operationLock.unlock();
        return nmr;
    }

    public void refreshCurrentAccountId(){
        this.currentAccountId = getCurrentAccountId();
    }

    public void refreshCurrentOperationId(){
        this.currentOperationId = getCurrentOperationId();
    }

    /**
     * Checks if database has the given account (normal mode)
     * @param account account number to be checked
     * @return true if database has given account number, false otherwise
     */
    public boolean hasAccount(int account){
        boolean result;
        if(cache.get(account) != null) return true;
        try {
            Statement s = rawDataSource.getConnection().createStatement();
            ResultSet res = s.executeQuery(
                    "SELECT ACCOUNT_ID FROM APP.ACCOUNTS WHERE ACCOUNT_ID = "+account);
            result = res.next();
        } catch (SQLException ex) {
            return false;
        }

        return result;
    }

    /**
     * Checks if database has the given account (recovery mode)
     * @param account account number to be checked
     * @param con connection to be used
     * @return true if database has given account number, false otherwise
     */
    public boolean hasAccount(int account, Connection con){
        boolean result;
        if(cache.get(account) != null) return true;
        try {
            Statement s = con.createStatement();
            ResultSet res = s.executeQuery(
                    "SELECT ACCOUNT_ID FROM APP.ACCOUNTS WHERE ACCOUNT_ID = "+account);
            result = res.next();
        } catch (SQLException ex) {
            return false;
        }

        return result;
    }

    private static String buildDBName(String name) {
        return new StringBuilder(DB_PATH)
                            .append(File.separatorChar)
                            .append(name)
                            .append(File.separatorChar)
                            .append(DB_FILENAME)
                            .toString();
    }

    /**
     * Returns operations executed after a given id
     * @param n_id operation identifier delimiter
     * @return list of bank operations
     */
    public List<BankOperation> getOperationsAfter(int n_id){
        List<BankOperation> op_list = new ArrayList<>();

        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT * FROM APP.OPERATIONS where OP_ID >= "+n_id+" ORDER BY OP_ID DESC")) {

            while (res.next()) {
                switch(res.getInt("OP_TYPE")){
                    //MOVEMENT
                    case 1:
                        op_list.add(new BankOperation.Movement(res.getInt("OP_ID"), res.getInt("MV_AMOUNT"), res.getInt("FROM_CURRENT_BALANCE"), res.getString("FROM_ACCOUNT_ID")));
                        break;
                    //TRANSFER
                    case 2:
                        op_list.add(new BankOperation.Transfer(res.getInt("OP_ID"), res.getInt("MV_AMOUNT"),
                                res.getString("FROM_ACCOUNT_ID"), res.getString("TO_ACCOUNT_ID"), res.getInt("FROM_CURRENT_BALANCE"),
                                res.getInt("TO_CURRENT_BALANCE")));
                        break;
                    //CREATE
                    case 3:
                        op_list.add(new BankOperation.Create(res.getInt("OP_ID"), res.getString("FROM_ACCOUNT_ID")));
                        break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return op_list;
    }
}
