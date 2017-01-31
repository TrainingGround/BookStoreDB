import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.xmlbeans.impl.inst2xsd.util.Type;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


public class DBUtil {
    private Connection connection = null;
    public DBUtil() {


    }
    public void createConnection(String hostName, String dbName, String login, String password){
        try {
            String connectionData = "jdbc:postgresql://"+hostName+ dbName;
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

            try (Statement statement = connection.createStatement()) {
                statement.execute(statementStr);
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

        try (Statement statement = connection.createStatement()){
            statement.execute("set search_path to " + schema +";");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public JSONArray readAllData(String schemaName, String tableName){
        JSONArray jsonArray = new JSONArray();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = connection.createStatement().executeQuery("Select*From "+tableName+";")){
            statement.execute("set search_path to " + schemaName + ";");
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
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return jsonArray;
    }
    public void populateTable(ArrayList<HashMap<String, Object>> dataList, String tableName){
        //populate table
        //read column names from 1st row
        HashMap<String,Object> firstRow = dataList.get(0);
        String columnsViaComa = "";
        String questionSignsViaComa="";
        Iterator iterator = firstRow.keySet().iterator();
        while(iterator.hasNext()){
            columnsViaComa+=iterator.next();
            questionSignsViaComa+="?";
            if (iterator.hasNext()){
                columnsViaComa+=",";
                questionSignsViaComa+=",";
            }
        }
        String statement = "INSERT INTO "+ tableName+ " ( "+
                columnsViaComa+") VALUES (" + questionSignsViaComa+");";

        try (PreparedStatement prep = connection.prepareStatement(statement)){
            for (int i = 0; i<dataList.size(); i++){

                int j=0;
                for (String key: dataList.get(i).keySet()){
                    j++;
                    prep.setObject(j,dataList.get(i).get(key));
                }
                prep.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public XSSFWorkbook writeTableToExcel(String tableName){
        XSSFWorkbook workbook = new XSSFWorkbook();
        try (Statement statement= connection.createStatement();
             ResultSet resultSet= statement.executeQuery("SELECT*FROM "+tableName+";")) {
            ResultSetMetaData rsMD = resultSet.getMetaData();
            XSSFSheet sheet = workbook.createSheet(tableName);
            //create row with column titles
            XSSFRow row = sheet.createRow(1);
            XSSFCell cell;
            for (int counter=1; counter<rsMD.getColumnCount()+1;counter++){
                cell = row.createCell(counter);
                cell.setCellValue(rsMD.getColumnName(counter));
            }
            int j=2;//first row occupied with column titles
            while(resultSet.next()){
                row = sheet.createRow(j);
                for (int i=1; i<=rsMD.getColumnCount();i++){
                    cell = row.createCell(i);
                    if (rsMD.getColumnType(i)==Types.INTEGER)
                        cell.setCellValue(resultSet.getInt(rsMD.getColumnName(i)));
                    if (rsMD.getColumnType(i)==Types.VARCHAR)
                        cell.setCellValue(resultSet.getString(rsMD.getColumnName(i)));
                }
                j++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return workbook;
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
