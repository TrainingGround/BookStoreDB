import org.json.simple.JSONArray;
import org.junit.*;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TestMain {
    final static String TABLE_NAME = "BOOKS";
    final static String SCHEMA_NAME = "BookSchema";
    static DBUtil dbUtil = new DBUtil();
    public static void main(String[] args) {
        dbUtil.createConnection("localhost", "BookStoreDB", "postgres", "postgres");
        dbUtil.createSchema(SCHEMA_NAME);
        dbUtil.setSchema(SCHEMA_NAME);
        //dbUtil.dropSchema("TestSchema");
        createTable();
        populateTable();
        jsonTest();
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
                else map.put(TableFields.getFieldString(field), generateName());;
            //Here all the fields except Year are set as a single "word" TODO more sophisticated generator?
            dataList.add(map);
        }
        //populate table
        String statement = "INSERT INTO "+ TABLE_NAME+ " ( "+
                TableFields.getAllFieldsString()+") VALUES (" + questionGenerator()+");";
        try {
            for (int i = 0; i<numOfRows; i++){
                PreparedStatement prep = dbUtil.getConnection().prepareStatement(statement);
                int j=0;
                for (TableFields field: TableFields.values()){
                    j++;
                    prep.setObject(j,dataList.get(i).get(TableFields.getFieldString(field)));
                }
                prep.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


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
    public static String questionGenerator(){
        String string = "";
        for (int counter = 0; counter<TableFields.values().length; counter++)
            if(counter!=0)string+=",?";
                    else string+="?";
        return string;
    }
    public static int generateYear(){
        Random random = new Random();
        int year = 1900+random.nextInt(116);
        return year;
    }
    public static String generateName(){
        Random random = new Random();
        char[] word = new char[5+random.nextInt(5)];
        for(int counter=0;counter<word.length;counter++){
            word[counter]=(char)('a'+random.nextInt(26));
        }
        return new String(word);
    }

    enum TableFields{
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
        public static String getAllFieldsString(){ //return string with all enum fields via coma
            String string="";
            for (TableFields field:TableFields.values())
                if (string!="") string+=","+ TableFields.getFieldString(field);
                    else string+= TableFields.getFieldString(field);
            return string;
        }

    }
}
