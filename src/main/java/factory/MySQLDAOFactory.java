package factory;

import dao.*;
import dto.Cliente;
import dto.Producto;
import interfaces.InterfaceClienteDAO;
import interfaces.InterfaceDAO;
import interfaces.InterfaceProductoDAO;

import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;

public class MySQLDAOFactory extends DAOFactory {

    //JDBC driver y base de datos URL
    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/integrador1";

    //base de datos credenciales
    private static final String USER = "root";
    private static final String PASS = "";

    private static MySQLDAOFactory instancia;

    //Constructor privado para evitar crear un new una nueva instancia
    private MySQLDAOFactory() {
        Locale.setDefault(new Locale("en", "US"));
    }

    public static MySQLDAOFactory getInstancia() {
        if(instancia == null)
            instancia = new MySQLDAOFactory();
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
            conn = MySQLDAOFactory.conectar();
            String query = "SELECT * " +
                    "FROM information_schema.tables " +
                    "WHERE table_schema = 'integrador1' " +
                    "AND table_name = '" + table + "'";
            PreparedStatement st = conn.prepareStatement(query);
            ResultSet rs = st.executeQuery();
            if (rs.next())
                return true;
            else
                return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            conn.close();
        }
    }

    public InterfaceClienteDAO<Cliente> getClienteDAO() throws Exception {
        return new MySQLClienteDAO();
    }

    public InterfaceDAO getFacturaDAO() throws Exception {
        return new MySQLFacturaDAO();
    }

    public InterfaceProductoDAO<Producto> getProductoDAO() throws Exception {
        return new MySQLProductoDAO();
    }

    public InterfaceDAO getFacturaProductoDAO() throws Exception {
        return new MySQLFacturaProductoDAO();
    }

    // SQL especificas
    @Override
    public ArrayList<Cliente> listAllClient() throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();

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
