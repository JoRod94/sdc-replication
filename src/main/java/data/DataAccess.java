package data;

import java.io.File;
import java.sql.*;
import java.util.List;

import org.apache.derby.jdbc.EmbeddedDataSource;

//atualizar balance das contas ao fazer operation
public class DataAccess {
    EmbeddedDataSource rawDataSource;

    private static final String DB_PATH = "./src/main/resources";
    private static final String DB_FILENAME = "BankData";
    private static final String[] OP_TYPES = {"movement", "transfer", "create"};

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
                + "CLIENT_ALIAS VARCHAR(20), "
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
        for(String ot : OP_TYPES)
            dbUpdate("insert into OPERATION_TYPE (OP_DESIGNATION) values (\""+ot+"\")");
    }

    public void dropTable(String tablename) {
        dbUpdate("DROP TABLE " + tablename);
    }

    public int logOperation(String op_type, int mv_amount, int client_id, int current_balance){
        int generated_id = 0;
        PreparedStatement stmt = null;
        try {
            stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into OPERATIONS (OP_TYPE, MV_AMOUNT, CLIENT_ID, CURRENT_BALANCE, TIMESTAMP) values (?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, op_type);
            stmt.setInt(2, mv_amount);
            stmt.setInt(3, client_id);
            stmt.setInt(4, current_balance);
            stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            generated_id = getGeneratedKey(stmt);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return generated_id;
    }

    public int insertNewAccount(String client_alias, int balance){
        int generated_id = 0;
        PreparedStatement stmt = null;
        try {
            stmt = rawDataSource.getConnection().prepareStatement(
                    "insert into ACCOUNTS (CLIENT_ALIAS, BALANCE, TIMESTAMP) values (?,?,?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, client_alias);
            stmt.setInt(2, balance);
            stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            stmt.execute();
            generated_id = getGeneratedKey(stmt);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return generated_id;
    }

    public int getGeneratedKey(Statement stmt){
        ResultSet rs = null;
        try {
            rs = stmt.getGeneratedKeys();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public int updateBalance(int client_id, int amount){
        int current_balance = getClientBalance(client_id);
        int new_balance = current_balance + amount;
        if(new_balance >= 0) {
            dbUpdate("update ACCOUNTS set BALANCE = "+ new_balance + " where CLIENT_ID = " + client_id);
            return new_balance;
        }
        return -1;
    }

    public int updateBalance(String client_alias, int amount){
        int current_balance = getClientBalance(client_alias);
        int new_balance = current_balance + amount;
        if(new_balance >= 0){
            dbUpdate("update ACCOUNTS set BALANCE = " + new_balance + " where CLIENT_ALIAS = "+"\'"+client_alias+"\'");
            return new_balance;
        }
        return -1;
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

    public int getClientBalance(String client_alias){
        int balance = 0;
        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT BALANCE FROM APP.ACCOUNTS WHERE CLIENT_ALIAS = "+"\'"+client_alias+"\'")) {

            if (res.next())
                balance = res.getInt("BALANCE");
        } catch (SQLException ex) {
            ex.printStackTrace();
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

    public void removeAccount(String client_alias){
        dbUpdate("delete from ACCOUNTS where CLIENT_ALIAS = " + client_alias);
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
                            .append("\tAlias: " + res.getString("CLIENT_ALIAS"))
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

    public String getLastClientOperations(String client_alias, int n) throws SQLException {
        return getLastClientOperations(getClientId(client_alias), n);
    }

    public int getClientId(String client_alias) throws SQLException {
        try (
                Statement s = rawDataSource.getConnection().createStatement();
                ResultSet res = s.executeQuery(
                        "SELECT * FROM APP.ACCOUNTS where CLIENT_ALIAS = \'"+client_alias+"\'")) {
            while (res.next()) {
                return Integer.parseInt(res.getString("CLIENT_ID"));
            }
        }
        return -1;
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

    public List<Object> returnOperationsFromN(int n_id){

        return null;
    }
}
