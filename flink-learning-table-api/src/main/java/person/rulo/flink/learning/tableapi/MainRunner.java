package person.rulo.flink.learning.tableapi;

import person.rulo.flink.learning.tableapi.connector.JDBCSQLConnector;

public class MainRunner {

    public static void main(String[] args) throws Exception {

//        TableCreator tableCreator = new TableCreator();
//        tableCreator.createTable();

        JDBCSQLConnector jdbcsqlConnector = new JDBCSQLConnector();
//        jdbcsqlConnector.createMySQLTable();
        jdbcsqlConnector.mySQL2MySQL();
//        jdbcsqlConnector.createPostgreSQLTable();
    }

}
