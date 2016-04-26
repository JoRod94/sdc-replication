package data;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import bank.*;
import org.apache.derby.jdbc.EmbeddedDataSource;

// talvez meter enum na invocation
public class DataAccess {
    EmbeddedDataSource rawDataSource;

    private static final String DB_PATH = "./src/main/resources/db";
    private static final String DB_FILENAME = "BankData";
    public enum OP_TYPES {MOVEMENT, TRANSFER, CREATE};
    private int currentAccountId, currentOperationId;
    private CacheManager<Account> cache;

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

    public void dbUpdate(String query) {
        try {
            tryDbUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void tryDbUpdate(String query) throws SQLException {
        Statement s = null;
        try {
            s = rawDataSource.getConnection().createStatement();
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

        populateOperationType();
    }

    public void populateOperationType(){
        for(OP_TYPES ot : OP_TYPES.values())
            dbUpdate("insert into OPERATION_TYPE (OP_TYPE, OP_DESIGNATION) values " +
                    "("+(ot.ordinal()+1)+",\'"+ot.name()+"\')");
    }

    public void dropTable(String tablename) throws SQLException {
        tryDbUpdate("DROP TABLE " + tablename);
    }

    public int makeMovement(int op_id, int mv_amount, int account_id, int final_balance, boolean recovery){
        int generated_id = 0;

        try {
            PreparedStatement stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into OPERATIONS (OP_ID, OP_TYPE, MV_AMOUNT, FROM_ACCOUNT_ID, FROM_CURRENT_BALANCE, TIMESTAMP) " +
                            "values (?,?,?,?,?,?)");

            if(recovery) {
                stmt.setInt(1, generated_id = op_id);
            } else {
                stmt.setInt(1, generated_id = currentOperationId++);
            }
            stmt.setInt(2, OP_TYPES.valueOf("MOVEMENT").ordinal()+1);
            stmt.setInt(3, mv_amount);
            stmt.setInt(4, account_id);
            stmt.setInt(5, final_balance);
            stmt.setTimestamp(6, new Timestamp(System.currentTimeMillis()));

            stmt.execute();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if(!recovery) updateBalance(account_id, final_balance);

        return generated_id;
    }

    public int makeTransfer(int op_id, int tr_amount, int from_account, int to_account, int from_final_balance,
                            int to_final_balance, boolean recovery){
        int generated_id = 0;

        try {
            PreparedStatement stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into OPERATIONS (OP_ID, OP_TYPE, MV_AMOUNT, FROM_ACCOUNT_ID, TO_ACCOUNT_ID, FROM_CURRENT_BALANCE, " +
                            "TO_CURRENT_BALANCE, TIMESTAMP) values (?,?,?,?,?,?,?,?)");
            if(recovery){
                stmt.setInt(1, generated_id = op_id);
            } else {
                stmt.setInt(1, generated_id = currentOperationId++);
            }
            stmt.setInt(2, OP_TYPES.valueOf("TRANSFER").ordinal()+1);
            stmt.setInt(3, tr_amount);
            stmt.setInt(4, from_account);
            stmt.setInt(5, to_account);
            stmt.setInt(6, from_final_balance);
            stmt.setInt(7, to_final_balance);
            stmt.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if(!recovery) {
            updateBalance(from_account, from_final_balance);
            updateBalance(to_account, to_final_balance);
        }

        return generated_id;
    }

    public int logNewAccount(int op_id, int account_id, int current_balance, boolean recovery){
        int generated_id = 0;

        try {
            PreparedStatement stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into OPERATIONS (OP_ID, OP_TYPE, FROM_ACCOUNT_ID, FROM_CURRENT_BALANCE, TIMESTAMP) " +
                            "values (?,?,?,?,?)");
            if(recovery) {
                stmt.setInt(1, generated_id = op_id);
            } else {
                stmt.setInt(1, generated_id = currentOperationId++);
            }
            stmt.setInt(2, OP_TYPES.valueOf("CREATE").ordinal()+1);
            stmt.setInt(3, account_id);
            stmt.setInt(4, current_balance);
            stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return generated_id;
    }

    public int makeNewAccount(int account_nmr, int balance, boolean recovery){
        int generated_id = 0;

        try {
            PreparedStatement stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into ACCOUNTS (ACCOUNT_ID, BALANCE, TIMESTAMP) values (?,?,?)");
            if(recovery) {
                stmt.setInt(1, generated_id = account_nmr);
                stmt.setInt(2, balance);
            } else {
                stmt.setInt(1, generated_id = currentAccountId++);
                stmt.setInt(2, balance);
            }
            stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        cache.add(new Account(account_nmr, balance));
        if(!recovery) logNewAccount(0, generated_id, 0, false);


        return generated_id;
    }

    public void updateBalance(int account_id, int final_amount){
        dbUpdate("update ACCOUNTS set BALANCE = "+ final_amount + " where ACCOUNT_ID = " + account_id);
        cache.add(new Account(account_id, final_amount));
    }

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

    public String getOperationTypes() throws SQLException {
        StringBuilder a = new StringBuilder();
        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT * FROM APP.OPERATION_TYPE")) {
            a.append("List of operation types: \n");
            while (res.next()) {
                a.append("Id: " + res.getString("OP_TYPE"))
                        .append("\tType: " + res.getString("OP_DESIGNATION")+"\n");
            }
        }
        return a.toString();
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

    // TODO: É necessário excluir o operação de criação de conta?
    public String getLastAccountOperations(int account_id, int n) {
        StringBuilder a = new StringBuilder();
        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT * FROM OPERATIONS where FROM_ACCOUNT_ID = "+account_id+" ORDER BY TIMESTAMP " +
                                "DESC FETCH FIRST "+n+" ROWS ONLY")) {

            while (res.next()) {
                a.append("Id: " + res.getString("OP_ID"))
                        .append("\tType: " + OP_TYPES.values()[res.getInt("OP_TYPE")-1].name())
                        .append("\tAmount: " + res.getString("MV_AMOUNT"))
                        .append("\tBalance: " + res.getString("FROM_CURRENT_BALANCE"))
                        .append("\tTimestamp: " + res.getString("TIMESTAMP").toString()+"\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if(a.length() == 0) a.append("No operations for account "+account_id);
        return a.toString();
    }

    private int getCurrentAccountId() {
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
        return nmr;
    }

    public int getCurrentOperationId(){
        int nmr = 1;
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
        return nmr;
    }

    public void refreshCurrentAccountId(){
        this.currentAccountId = getCurrentAccountId();
    }

    public void refreshCurrentOperationId(){
        this.currentOperationId = getCurrentOperationId();
    }

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

    private static String buildDBName(String name) {
        return new StringBuilder(DB_PATH)
                            .append(File.separatorChar)
                            .append(name)
                            .append(File.separatorChar)
                            .append(DB_FILENAME)
                            .toString();
    }

    //talvez arranjar para não ter o tipo de operações hardcoded no switch
    //rever
    public List<BankOperation> getOperationsAfter(int n_id){
        List<BankOperation> op_list = new ArrayList<>();

        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT * FROM APP.OPERATIONS where OP_ID >= "+n_id+" ORDER BY TIMESTAMP DESC")) {

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
