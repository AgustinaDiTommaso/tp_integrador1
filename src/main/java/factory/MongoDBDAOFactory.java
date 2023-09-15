package factory;

import dao.PostgreSQL.PostgreSQLClienteDAO;
import dao.PostgreSQL.PostgreSQLFacturaDAO;
import dao.PostgreSQL.PostgreSQLFacturaProductoDAO;
import dao.PostgreSQL.PostgreSQLProductoDAO;
import dto.Cliente;
import dto.Producto;
import interfaces.InterfaceClienteDAO;
import interfaces.InterfaceFacturaDAO;
import interfaces.InterfaceFacturaProductoDAO;
import interfaces.InterfaceProductoDAO;

import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;

public class MongoDBDAOFactory extends DAOFactory {

    //JDBC driver y base de datos URL
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String DB_URL = "mongodb://miusuario:mipassword@localhost:27017/integrador1";

    //base de datos credenciales
    private static final String USER = "postgres";
    private static final String PASS = "postgres";

    private static MongoDBDAOFactory instancia;

    //Constructor privado para evitar crear un new una nueva instancia
    private MongoDBDAOFactory() {
        Locale.setDefault(new Locale("en", "US"));
    }

    public static MongoDBDAOFactory getInstancia() {
        if(instancia == null)
            instancia = new MongoDBDAOFactory();
        return instancia;
    }

    public static Connection conectar() throws Exception {
        Connection conexion;
        try {
            Class.forName(DRIVER).getDeclaredConstructor().newInstance();
            conexion = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException e) {
            throw e;
        }
        return conexion;
    }

    public static boolean checkIfExistsEntity(String table, Connection conn) throws SQLException {
        try {
            conn = MongoDBDAOFactory.conectar();
            String query = "SELECT EXISTS (" +
                    "SELECT 1 " +
                    "FROM information_schema.tables " +
                    "WHERE table_schema = ? " +
                    "AND table_name = ?)";
            PreparedStatement st = conn.prepareStatement(query);
            st.setString(1,"public");
            st.setString(2, table);
            ResultSet rs = st.executeQuery();
            rs.next();
            return rs.getBoolean(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            conn.close();
        }
    }

    public InterfaceClienteDAO<Cliente> getClienteDAO() throws Exception {
        return new PostgreSQLClienteDAO();
    }

    public InterfaceFacturaDAO getFacturaDAO() throws Exception {
        return new PostgreSQLFacturaDAO();
    }

    public InterfaceProductoDAO<Producto> getProductoDAO() throws Exception {
        return new PostgreSQLProductoDAO();
    }

    public InterfaceFacturaProductoDAO getFacturaProductoDAO() throws Exception {
        return new PostgreSQLFacturaProductoDAO();
    }

    // SQL especificas
    @Override
    public ArrayList<Cliente> listAllClient() throws Exception {
        Connection conexion = MongoDBDAOFactory.conectar();

        PreparedStatement st = conexion.prepareStatement(
                "SELECT * FROM cliente");
        ResultSet rs = st.executeQuery();

        ArrayList<Cliente> clientes = new ArrayList<>();
        Cliente c;
        while (rs.next()) {
            c = new Cliente(rs.getInt(1), rs.getString(2), rs.getString(3));
            clientes.add(c);
        }

        conexion.close();
        return clientes;
    }


}
