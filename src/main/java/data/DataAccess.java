package data;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import bank.*;
import org.apache.derby.jdbc.EmbeddedDataSource;

//meter a inserir log depois de fazer insertAccount
//nao é preciso fazer cuidado de balances
//makeMovement, makeTransfer, makeBalance
//talvez meter enum na invocation
//trocar client_id por account_id
public class DataAccess {
    EmbeddedDataSource rawDataSource;

    private static final String DB_PATH = "./src/main/resources";
    private static final String DB_FILENAME = "BankData";
    public enum OP_TYPES {MOVEMENT, TRANSFER, CREATE};

    public void initEDBConnection(String name, boolean drop_tables, boolean create_tables) throws SQLException {
        String dbName = buildDBName(name);
        rawDataSource = new EmbeddedDataSource();
        rawDataSource.setDatabaseName(dbName);

        File f = new File(dbName);

        if (!f.exists()) {
            rawDataSource.setCreateDatabase("create");
        } else {
            if (!f.isDirectory()) {
                rawDataSource.setCreateDatabase("create");
            }
        }

        if(drop_tables){
            dropTable("ACCOUNTS");
            dropTable("OPERATION_TYPE");
            dropTable("OPERATIONS");
        }
        if(create_tables){
            createAccountsTable();
            createOperationTypeTable();
            createOperationsTable();
        }
    }

    public void dbUpdate(String query) {
        Statement s = null;
        try {
            s = rawDataSource.getConnection().createStatement();
            s.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                s.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void createAccountsTable(){
        dbUpdate("create table ACCOUNTS ("
                + "CLIENT_ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
                + "BALANCE INTEGER,"
                + "TIMESTAMP TIMESTAMP)");
    }

    public void createOperationsTable(){
        dbUpdate("create table OPERATIONS ("
                + "OP_ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
                + "OP_TYPE INTEGER, "
                + "MV_AMOUNT INTEGER, "
                + "FROM_CLIENT_ID INTEGER, "
                + "TO_CLIENT_ID INTEGER, "
                + "FROM_CURRENT_BALANCE INTEGER, "
                + "TO_CURRENT_BALANCE INTEGER, "
                + "TIMESTAMP TIMESTAMP, "
                + "CONSTRAINT OP_TYPE_REF FOREIGN KEY (OP_TYPE) REFERENCES OPERATION_TYPE(OP_TYPE), "
                + "CONSTRAINT FROM_CLIENT_ID_REF FOREIGN KEY (FROM_CLIENT_ID) REFERENCES ACCOUNTS(CLIENT_ID), "
                + "CONSTRAINT TO_CLIENT_ID_REF FOREIGN KEY (CLIENT_ID) REFERENCES ACCOUNTS(CLIENT_ID))");
    }

    public void createOperationTypeTable(){
        dbUpdate("create table OPERATION_TYPE ("
                + "OP_TYPE INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
                + "OP_DESIGNATION VARCHAR(20))");

        populateOperationType();
    }

    public void populateOperationType(){
        for(OP_TYPES ot : OP_TYPES.values())
            dbUpdate("insert into OPERATION_TYPE (OP_DESIGNATION) values (\""+ot.name()+"\")");
    }

    public void dropTable(String tablename) {
        dbUpdate("DROP TABLE " + tablename);
    }

    //discutir se é apropriado receber o saldo final já no recovery
    public void recoverOperation(BankOperation bo, int account_id){

        if(bo instanceof CreateOperation) insertNewAccount();
        else if(bo instanceof MovementOperation){
            MovementOperation mo = (MovementOperation) bo;
            makeMovement(mo.getAmount(), /*mo.getAccountId()*/account_id, mo.getFinalBalance());
        }
        else if(bo instanceof TransferOperation) {
            TransferOperation to = (TransferOperation) bo;
            makeTransfer(to.getAmount(), Integer.parseInt(to.getAccountFrom()), Integer.parseInt(to.getAccountTo()),
                    to.getFinalBalanceFrom(), to.getFinalBalanceTo());
        }
    }

    public int makeMovement(int mv_amount, int account_id, int final_balance){
        int generated_id = 0;

        try {
            PreparedStatement stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into OPERATIONS (OP_TYPE, MV_AMOUNT, FORM_CLIENT_ID, FROM_CURRENT_BALANCE, TIMESTAMP) " +
                            "values (?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setInt(1, OP_TYPES.valueOf("MOVEMENT").ordinal()); //talvez tenha de fazer + 1
            stmt.setInt(2, mv_amount);
            stmt.setInt(3, account_id);
            stmt.setInt(4, final_balance);
            stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            generated_id = getGeneratedKey(stmt);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        updateBalance(account_id, final_balance);

        return generated_id;
    }

    public int makeTransfer(int tr_amount, int from_account, int to_account, int from_final_balance,
                            int to_final_balance){
        int generated_id = 0;

        try {
            PreparedStatement stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into OPERATIONS (OP_TYPE, MV_AMOUNT, FORM_CLIENT_ID, TO_CLIENT_ID, FROM_CURRENT_BALANCE, " +
                            "TO_CURRENT_BALANCE, TIMESTAMP) values (?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setInt(1, OP_TYPES.valueOf("TRANSFER").ordinal()); //talvez tenha de fazer + 1
            stmt.setInt(2, tr_amount);
            stmt.setInt(3, from_account);
            stmt.setInt(4, to_account);
            stmt.setInt(5, from_final_balance);
            stmt.setInt(6, to_final_balance);
            stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            generated_id = getGeneratedKey(stmt);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        updateBalance(from_account, from_final_balance);
        updateBalance(to_account, to_final_balance);

        return generated_id;
    }

    public int logNewAccount(int account_id, int current_balance){
        int generated_id = 0;

        try {
            PreparedStatement stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into OPERATIONS (OP_TYPE, CLIENT_ID, CURRENT_BALANCE, TIMESTAMP) values (?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setInt(1, OP_TYPES.valueOf("CREATE").ordinal());
            stmt.setInt(2, account_id);
            stmt.setInt(3, current_balance);
            stmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            generated_id = getGeneratedKey(stmt);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return generated_id;
    }

    public int insertNewAccount(){
        int generated_id = 0;

        try {
            PreparedStatement stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into ACCOUNTS (BALANCE, TIMESTAMP) values (?,?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setInt(1, 0);
            stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            generated_id = getGeneratedKey(stmt);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        logNewAccount(generated_id, 0);
        return generated_id;
    }

    public int getGeneratedKey(Statement stmt){
        try {
            ResultSet rs = stmt.getGeneratedKeys();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public void updateBalance(int client_id, int final_amount){
        dbUpdate("update ACCOUNTS set BALANCE = "+ final_amount + " where CLIENT_ID = " + client_id);
    }

    public int getClientBalance(int client_id){
        int balance = 0;
        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT BALANCE FROM APP.ACCOUNTS WHERE CLIENT_ID = " + client_id)) {

            if (res.next())
                balance = res.getInt("BALANCE");
        } catch (SQLException ex) {
            return balance;
        }
        return balance;
    }

    //meter a devolver true ou false
    public void removeOperation(int op_id) throws SQLException {
        dbUpdate("delete from OPERATIONS where OP_ID = " + op_id);
    }

    public void removeAccount(int client_id){
        dbUpdate("delete from ACCOUNTS where CLIENT_ID = " + client_id);
    }


    public String getOperationLogs() throws SQLException {
        StringBuilder a = new StringBuilder();
        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT * FROM APP.OPERATIONS")) {
            a.append("List of operation entries: \n");
            while (res.next()) {
                a.append("Id: " + res.getString("OP_ID"))
                        .append("\tType: " + res.getString("OP_TYPE"))
                        .append("\tAmount: " + res.getString("MV_AMOUNT"))
                        .append("\tClient: " + res.getString("CLIENT_ID"))
                        .append("\tBalance: " + res.getString("CURRENT_BALANCE"))
                        .append("\tTimestamp: " + res.getString("TIMESTAMP").toString()+"\n");
            }
        }
        return a.toString();
    }


    public String getClientAccountsInfo(){
        StringBuilder a = new StringBuilder();
        try {
            try (
                    Statement s = rawDataSource.getConnection().createStatement();
                    ResultSet res = s.executeQuery(
                            "SELECT * FROM APP.ACCOUNTS")) {
                a.append("List of client accounts entries: \n");
                while (res.next()) {
                    a.append("\tID: " + res.getString("CLIENT_ID"))
                            .append("\tBalance: " + res.getString("BALANCE"))
                            .append("\tTimestamp: " + res.getString("TIMESTAMP").toString()+"\n");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return a.toString();
    }

    public String getLastClientOperations(int client_id, int n) throws SQLException {
        StringBuilder a = new StringBuilder();
        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT * FROM APP.OPERATIONS where CLIENT_ID = "+client_id+" ORDER BY TIMESTAMP DESC FETCH FIRST "+n+" ROWS ONLY")) {
            a.append("List of the last n operations for client "+client_id+": \n");
            while (res.next()) {
                a.append("Id: " + res.getString("OP_ID"))
                        .append("\tType: " + res.getString("OP_TYPE"))
                        .append("\tAmount: " + res.getString("MV_AMOUNT"))
                        .append("\tBalance: " + res.getString("CURRENT_BALANCE"))
                        .append("\tTimestamp: " + res.getString("TIMESTAMP").toString()+"\n");
            }
        }
        return a.toString();
    }

    int getCurrentAccountId() {
        int nmr = 1;
        try (
                Statement s = rawDataSource.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet res = s.executeQuery(
                        "SELECT CLIENT_ID FROM APP.ACCOUNTS")) {

            if (res.last()) {
                nmr = Integer.parseInt(res.getString("CLIENT_ID"));
            }
        } catch (SQLException ex) {
            return nmr;
        }
        return nmr;
    }

    int getCurrentOperationId(){
        int nmr = 1;
        try (
                Statement s = rawDataSource.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet res = s.executeQuery(
                        "SELECT OP_ID FROM APP.OPERATIONS")) {

            if (res.last()) {
                nmr = Integer.parseInt(res.getString("OP_ID"));
            }
        } catch (SQLException ex) {
            return nmr;
        }
        return nmr;
    }
    private static String buildDBName(String name) {
        return new StringBuilder(DB_PATH)
                            .append(File.pathSeparator)
                            .append(name)
                            .append(File.pathSeparator)
                            .append(DB_FILENAME)
                            .toString();
    }

    //talvez arranjar para não ter o tipo de operações hardcoded no switch
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
                        op_list.add(new MovementOperation(res.getInt("OP_ID"), res.getInt("MV_AMOUNT"), res.getInt("FROM_CURRENT_BALANCE")));
                        break;
                    //TRANSFER
                    case 2:
                        op_list.add(new TransferOperation(res.getInt("OP_ID"), res.getInt("MV_AMOUNT"),
                                res.getString("FROM_CLIENT_ID"), res.getString("TO_CLIENT_ID"), res.getInt("FROM_CURRENT_BALANCE"),
                                res.getInt("TO_CURRENT_BALANCE")));
                        break;
                    //CREATE
                    case 3:
                        op_list.add(new CreateOperation(res.getInt("OP_ID"), res.getString("FROM_CLIENT_ID")));
                        break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return op_list;
    }
}
