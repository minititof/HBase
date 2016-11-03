import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Scanner;

public class dbREPL {

    private static Configuration CONF = null;

    //Initialization
    static {
        CONF = HBaseConfiguration.create();
    }

    //Table creation
    private static void creatTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(CONF);
            //If the table doesn't exist ...
            if (admin.tableExists(tableName) == false) {
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                //2 columns, one for the info and one for the friends
                tableDesc.addFamily( new HColumnDescriptor("info"));
                tableDesc.addFamily( new HColumnDescriptor("friends"));
                admin.createTable(tableDesc);
            }
            //Else, the table exists!
            else System.out.println("Table already exists!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Insert a row (similar to external tutorial)
    private static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) {
        try {
            //New table
            HTable table = new HTable(CONF, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            System.out.println("insert record " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //Insert a new person to the table
    private static void addPerson(String tableName) {
        //Scanner to get user input
        Scanner scanner = new Scanner(System.in);
        //Basic information about a user: name, date of birth, address, friends, name of the bff
        String name, birthDate, address, bffName, friends;

        //Getting the name
        System.out.println("Name: ");
        name = scanner.nextLine();

        //Getting the date of birth
        System.out.print("Date of birth (DD/MM/YY): ");
        birthDate = scanner.nextLine();

        //Getting the address
        System.out.print("Address: ");
        address = scanner.nextLine();

        //Getting the friends' names
        System.out.println("Friends (separated by ;):");
        friends = scanner.nextLine();

        //Getting the bff
        bffName = "";
        //Mandatory, so we check if it's not empty and ask again until the user inputs a name
        while (bffName == "") {
            System.out.println("BFF name: ");
            bffName = scanner.nextLine();
        }

        //Add record for this person
        dbREPL.addRecord(tableName, name, "information", "birthDate", birthDate);
        dbREPL.addRecord(tableName, name, "information", "address", address);
        dbREPL.addRecord(tableName, name, "friends", "bffName", bffName);
        dbREPL.addRecord(tableName, name, "friends", "others", friends);
    }

    //REPL
    public static void main(String[] args) {
        String tableName = "cbartier";
        //Welcoming the user when the app starts
        System.out.println("Welcome to Awesome People, the SNS only for truly amazing people!");
        //Creating the table 'cbartier
        creatTable(tableName);
        //Scanner to get user input, R is for Read
        Scanner scanner = new Scanner(System.in);
        String choice;

        //L is for Loop
        while(true) {
            //P is for Print
            System.out.println("1. Add a new person - 2. Exit (enter 1 or 2)");
            //Reading
            choice = scanner.nextLine();
            //E is for Eval
            if (choice == "1")
                addPerson(tableName);
            else if (choice == "2")
                //Ends the loop
                break;
            else
                //If the user didn't write 1 or 2, he must try again
                System.out.println("There seems to be an error, please try again. Please press 1 to add a new person or 2 to exit.");
        }
    }
}
