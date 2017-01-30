import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.simple.JSONArray;
import org.junit.*;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class TestMain {
    private final static String TABLE_NAME = "BOOKS";
    private final static String SCHEMA_NAME = "BookSchema";
    private static DBUtil dbUtil = new DBUtil();
    public static void main(String[] args) {
        dbUtil.createConnection("localhost:5432/", "BookStoreDB", "postgres", "postgres");
        dbUtil.createSchema(SCHEMA_NAME);
        dbUtil.setSchema(SCHEMA_NAME);
        //dbUtil.dropSchema("TestSchema");
        createTable();
        populateTable();
        jsonTest();
        copyDBtoExcel();
        dbUtil.closeConnection();

    }
    @Test
    public static void createTable(){//create table if it does not exist
        dbUtil.executeStatement("CREATE TABLE IF NOT EXISTS "+TABLE_NAME+"("+
                "ID SERIAL  PRIMARY KEY,"+
                TableFields.getFieldString(TableFields.Author)+" TEXT,"+
                TableFields.getFieldString(TableFields.Name)+" TEXT,"+
                TableFields.getFieldString(TableFields.Year)+" INT,"+
                TableFields.getFieldString(TableFields.Text)+" TEXT);");
    }
    @Test
    public static void populateTable(){
        int numOfRows = 1000;
        ArrayList<HashMap<String, Object>> dataList = new ArrayList<HashMap<String, Object>>();
        //fill array
        for (int i = 0; i<numOfRows; i++){
            HashMap<String, Object> map = new HashMap<String, Object>();
            for (TableFields field:TableFields.values())
                if (field == TableFields.Year)
                    map.put(TableFields.getFieldString(field), generateYear());
                else map.put(TableFields.getFieldString(field), generateName());
            //Here all the fields except Year are set as a single "word"
            dataList.add(map);
        }
        dbUtil.populateTable(dataList, TABLE_NAME);
    }
    @Test
    public static void  jsonTest(){
        JSONArray jArray = dbUtil.readAllData(SCHEMA_NAME,TABLE_NAME);
        try {
            FileWriter file = new FileWriter("JSON_FILE.txt");
            file.write(jArray.toJSONString());
            System.out.println("JSON file created");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public static void copyDBtoExcel(){
        try {
            FileOutputStream outStream = new FileOutputStream(new File("excel_FILE.xlsx"));
            dbUtil.writeTableToExcel(TABLE_NAME).write(outStream);
            outStream.close();
            System.out.println("EXCEL file was written");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static int generateYear(){
        Random random = new Random();
        return 1900+random.nextInt(116);
    }
    private static String generateName(){
        Random random = new Random();
        char[] word = new char[5+random.nextInt(5)];
        for(int counter=0;counter<word.length;counter++){
            word[counter]=(char)('a'+random.nextInt(26));
        }
        return new String(word);
    }

    private enum TableFields{
        Author, Name, Year, Text;
        public static String getFieldString(TableFields field){
            String fieldString ="";
            switch (field){
                case Author:
                    fieldString = "Author";
                    break;
                case Name:
                    fieldString = "Name";
                    break;
                case Year:
                    fieldString = "Year";
                    break;
                case Text:
                    fieldString ="Text";
                    break;
                default:
                    fieldString="";
                    break;
            }
            return fieldString;
        }
    }
}
