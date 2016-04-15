import java.sql.SQLException;

/**
 * Created by paulo on 14/04/16.
 */
public class TestDataAccess {
    public static void main(String[] args){
        DataAccess da = new DataAccess();
        try {
            da.initEDBConnection(true, true);

            System.out.println("Generated id: "+da.insertNewAccount("Paulo", 1000));
            System.out.println("Generated id: "+da.insertNewAccount("Peulo", 1000));
            System.out.println("Generated id: "+da.insertNewAccount("Pvulo", 1000));


            for(int i = 0; i<15; i++)
                System.out.println("Generated op_id: "+da.logOperation("OP", 0,1, 100));

            System.out.println(da.getClientAccountsInfo());
            System.out.println(da.getLastClientOperations(1, 5));
            System.out.println(da.getLastClientOperations("Paulo", 2));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}