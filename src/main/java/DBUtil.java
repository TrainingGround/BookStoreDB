import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.sql.*;

public class DBUtil {
    private Connection connection = null;
    public DBUtil() {


    }
    public void createConnection(String hostName, String dbName, String login, String password){
        try {
            String connectionData = "jdbc:postgresql://"+hostName+":5432/"+ dbName;//TODO hardcoded port?
            Class.forName("org.postgresql.Driver");

            connection = DriverManager.getConnection(
                    connectionData, login, password);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (connection!=null) System.out.println("Connected");
                else System.out.println("Still Null");
    }

    public void executeStatement(String statementStr){
        if (connection!=null){
            try {
                Statement statement = connection.createStatement();
                statement.execute(statementStr);
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else System.out.println("No connection to DB");
    }
    public void createSchema(String schema){
        System.out.println("Schema "+schema+" to be created...");
        executeStatement("CREATE SCHEMA IF NOT EXISTS "+schema);
    }
    public void setSchema(String schema){
        try {
            Statement statement = connection.createStatement();
            statement.execute("set search_path to " + schema +";");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public JSONArray readAllData(String schemaName, String tableName){
        JSONArray jsonArray = new JSONArray();
        try {
            Statement statement = connection.createStatement();
            statement.execute("set search_path to " + schemaName + ";");
            ResultSet resultSet = statement.executeQuery("Select*From "+tableName+";");
            ResultSetMetaData resMD = resultSet.getMetaData();

            while (resultSet.next()){
                int numColumns = resMD.getColumnCount();
                JSONObject jsonObject = new JSONObject();
                for (int i=1;i<numColumns+1;i++){
                    String columnName = resMD.getColumnName(i);
                    if (resMD.getColumnType(i)== Types.VARCHAR)
                        jsonObject.put(columnName,resultSet.getString(columnName));
                    if(resMD.getColumnType(i)==Types.INTEGER)
                        jsonObject.put(columnName,resultSet.getInt(columnName));
                }
                jsonArray.add(jsonObject);
            }
            statement.close();
            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return jsonArray;
    }
    public Connection getConnection(){
        return connection;
    }
    public void dropSchema(String schema){
        System.out.println("Schema "+schema+" to be droped...");
        executeStatement("DROP SCHEMA "+schema+" RESTRICT");
    }
    public void closeConnection(){
        try {
            System.out.println("Connection closed");
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
