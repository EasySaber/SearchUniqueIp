import java.io.*;
import java.sql.*;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;

public class ProcessedIp {
    private static final Logger logger = Logger.getLogger(ProcessedIp.class.getName());
    static Connection connection;

    public static void main(String[] args) throws IOException {

        Handler fileHandler = new FileHandler();
        logger.addHandler(fileHandler);

        ProcessedIp bd = new ProcessedIp();
        String ex;
        boolean q = true; //Выход

        logger.info("Start program.");
        do {
            Scanner sc = new Scanner(System.in);
            System.out.println("Enter the file path 'e:\\Ip_addresses' ('q'->exit): ");
            ex = sc.nextLine();

            if(ex.equals("q")) {
                logger.info("Enter 'q' -> Exit.");
                break;
            }

            File fileIp = new File(ex);
            if(fileIp.exists()){
                logger.info("File: '" + ex + "' is found." );
                q = false;
                bd.connectBD();     //Подключение к БД
                bd.dropTable();     //Удаление старой таблицы
                bd.createTable();   //Создание новой таблицы
                bd.insertIp(ex);    //Обработка и внесение записей в таблицу
                bd.disconnectBD();  //Отключение БД
            }
            else
                logger.warning("File: '" + ex + "' not found.");

        }while (q);
    }

    //Подключение к БД
    private void connectBD(){
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("JDBC:sqlite:UniqueIp.db");
            logger.info("BD connected.");
        }
        catch (Exception e){
            logger.warning(e.getMessage());
        }
    }

    //Отключение БД
    private void disconnectBD(){
        try {
            connection.close();
            logger.info("BD disconnected.");
        } catch (SQLException e) {
            logger.warning(e.getMessage());
        }
    }

    //Удаление старой таблицы
    private void dropTable() {
        String queryDropTable = "DROP TABLE IF EXISTS ip ; ";

        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(queryDropTable);
            statement.execute("VACUUM;");
            logger.info("Drop table: Ip.");
        }
        catch (SQLException e){
            logger.warning(e.getMessage());
        }

    }

    //Создание новой таблицы
    private void createTable() {
        String queryCreateTable = "CREATE TABLE ip( " +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "ip VARCHAR(15) UNIQUE);";

        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(queryCreateTable);
            logger.info("Create table: Ip.");
        }
        catch (SQLException e){
            logger.warning(e.getMessage());
        }
    }

    //Обработка и вставка в таблицу
    private void insertIp(String fileIp){
        try {
            BufferedReader readFile = new BufferedReader(new FileReader(fileIp));
            long i = 0, j = 0;
            String line;
            boolean end = false;

            Statement statement = connection.createStatement();
            logger.info("Start processing.");

            while (!end) {
                statement.execute("BEGIN TRANSACTION;");
                while (j < 1000000) {
                    if ((line = readFile.readLine()) == null) {
                        end = true;
                        logger.info("File ended.");
                        break;
                    }
                    statement.executeUpdate("INSERT OR IGNORE INTO ip (ip) VALUES ('" + line + "'); ");
                    j++;
                    i++;
                }
                statement.execute("COMMIT;");
                logger.info("Processed " + i + "lines.");
                j = 0;
            }

            readFile.close();

            ResultSet rs = statement.executeQuery("SELECT count(*) FROM ip;");
            logger.info("Found " + rs.getString(1) + " unique IP.");
            logger.info("End processing.");
        }
        catch (IOException | SQLException e){
            logger.warning(e.getMessage());
        }

    }

}

