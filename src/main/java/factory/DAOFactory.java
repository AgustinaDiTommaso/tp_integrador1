package factory;

import dto.Cliente;
import dto.Producto;
import interfaces.InterfaceClienteDAO;
import interfaces.InterfaceFacturaDAO;
import interfaces.InterfaceFacturaProductoDAO;
import interfaces.InterfaceProductoDAO;

import java.util.ArrayList;

public abstract class DAOFactory {

    public static final int MYSQL_JDBC = 1;

    public static DAOFactory getDAOFactory(int DB_Factory) {
        switch (DB_Factory) {
            case MYSQL_JDBC:
                return MySQLDAOFactory.getInstancia();
            default:
                return null;
        }
    }

    public abstract InterfaceClienteDAO<Cliente> getClienteDAO() throws Exception;

    public abstract InterfaceFacturaDAO getFacturaDAO() throws Exception;

    public abstract InterfaceProductoDAO<Producto> getProductoDAO() throws Exception;

    public abstract InterfaceFacturaProductoDAO getFacturaProductoDAO() throws Exception;

    // SQL especificas
    public abstract ArrayList<Cliente> listAllClient() throws Exception;


}
