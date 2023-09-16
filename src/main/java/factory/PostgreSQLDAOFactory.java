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

public class PostgreSQLDAOFactory extends DAOFactory {

    //JDBC driver y base de datos URL
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/integrador1";

    //base de datos credenciales
    private static final String USER = "postgres";
    private static final String PASS = "postgres";

    private static PostgreSQLDAOFactory instancia;

    //Constructor privado para evitar crear un new una nueva instancia
    private PostgreSQLDAOFactory() {
        Locale.setDefault(new Locale("en", "US"));
    }

    public static PostgreSQLDAOFactory getInstancia() {
        if(instancia == null)
            instancia = new PostgreSQLDAOFactory();
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
            conn = PostgreSQLDAOFactory.conectar();
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

}
