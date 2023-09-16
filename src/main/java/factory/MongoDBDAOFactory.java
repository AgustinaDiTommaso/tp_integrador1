package factory;

import java.util.Locale;

import dao.MongoDB.*;
import interfaces.*;
import dto.*;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import dao.MongoDB.MongoDBClienteDAO;

import java.util.logging.Level;
import java.util.logging.Logger;

public class MongoDBDAOFactory extends DAOFactory {

    //JDBC driver y base de datos URL
    public static final String DRIVER = "org.postgresql.Driver";
//    private static final String DB_URL = "localhost";
    private static final String DB_URL = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.0.1";
//    private static final String DB_URL = "mongodb://mongodb:mongodb@localhost:27017/integrador1";
    private static final String DB_NAME = "integrador1";
    private static final int DB_PORT = 27017;

    //base de datos credenciales
    private static final String USER = "mongodb";
    private static final String PASS = "mongodb";

    private static MongoDBDAOFactory instancia;

    //Constructor privado para evitar crear un new una nueva instancia
    private MongoDBDAOFactory() {
        Locale.setDefault(new Locale("en", "US"));
    }

    public static MongoDBDAOFactory getInstancia() {
        // eliminar los mensajes de log que emite el driver de mongo
        Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.WARNING);

        if(instancia == null)
            instancia = new MongoDBDAOFactory();
        return instancia;
    }


    public static MongoDatabase conectar() throws Exception {
        try {
            String uri_string = DB_URL;
//            String uri_string = "mongodb://" + USER + ":" + PASS + "@" + DB_URL + ":" + DB_PORT + "/" + DB_NAME;

            // Crear una instancia de MongoClientURI con la URL de conexión
            MongoClientURI uri = new MongoClientURI(uri_string);

            // Crear una instancia de MongoClient usando la URI
            MongoClient mongoClient = new MongoClient(uri);

            // Obtener una instancia de MongoDatabase para la base de datos deseada
            MongoDatabase database = mongoClient.getDatabase(DB_NAME);

            return database;

        } catch (Exception e) {
            throw new RuntimeException("Error al conectar a MongoDB: " + e.getMessage(), e);
        }
    }

    public static boolean checkIfExistsEntity(String collection, MongoDatabase database) throws Exception {
        for (String nombre : database.listCollectionNames()) {
            if (nombre.equals(collection)) {
                return true;
            }
        }
        return false;
    }

    public InterfaceClienteDAO<Cliente> getClienteDAO() throws Exception {
        return new MongoDBClienteDAO();
    }

    public InterfaceFacturaDAO getFacturaDAO() throws Exception {
        return new MongoDBFacturaDAO();
    }

    public InterfaceProductoDAO<Producto> getProductoDAO() throws Exception {
        return new MongoDBProductoDAO();
    }

    public InterfaceFacturaProductoDAO getFacturaProductoDAO() throws Exception {
        return new MongoDBFacturaProductoDAO();
    }

}
