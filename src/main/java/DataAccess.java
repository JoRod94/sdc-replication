import java.io.File;
import java.sql.*;

import org.apache.derby.jdbc.EmbeddedDataSource;

public class DataAccess {

    EmbeddedDataSource rawDataSource;

    public void initEDBConnection(boolean drop_tables, boolean create_tables) throws SQLException {

        rawDataSource = new EmbeddedDataSource();
        rawDataSource.setDatabaseName("./src/main/resources/BankData");

        File f = new File("./src/main/resources/BankData");

        if (!f.exists()) {
            rawDataSource.setCreateDatabase("create");
        } else {
            if (!f.isDirectory()) {
                rawDataSource.setCreateDatabase("create");
            }
        }

        if(drop_tables){
            dropTable("ACCOUNTS");
            dropTable("OPERATIONS");
        }
        if(create_tables){
            createAccountsTable();
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
                + "OP_TYPE VARCHAR(20), "
                + "MV_AMOUNT INTEGER, "
                + "CLIENT_ID INTEGER, "
                + "CURRENT_BALANCE INTEGER, "
                + "TIMESTAMP TIMESTAMP)");
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


}
