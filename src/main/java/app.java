import dto.Cliente;
import interfaces.InterfaceClienteDAO;
import interfaces.InterfaceDAO;
import interfaces.InterfaceProductoDAO;
import dto.Producto;
import factory.DAOFactory;

import java.util.ArrayList;
import java.util.Iterator;

public class app {

    private String nombre;
    public static void main(String[] args) throws Exception {

        DAOFactory MySQL_Factory = DAOFactory.getDAOFactory(DAOFactory.MYSQL_JDBC);
        InterfaceClienteDAO clienteDao = MySQL_Factory.getClienteDAO();
        InterfaceProductoDAO productoDao = MySQL_Factory.getProductoDAO();
        InterfaceDAO facturaDao = MySQL_Factory.getFacturaDAO();
        InterfaceDAO facturaProductoDao = MySQL_Factory.getFacturaProductoDAO();

        Producto p = productoDao.mayorRecaudacionPorProducto();
        System.out.println("---------------------------------------------------");
        System.out.println("----------- Producto de mayor recaudación ---------");
        System.out.println("---------------------------------------------------");
        System.out.println("Id \t Producto \t\t  valor \t Recaudación ");
        System.out.println(p + "\t\t $" +  p.getRecaudacion());
        System.out.println();

        ArrayList<Cliente> c = clienteDao.obtenerClientePorRecaudacion();
        Iterator<Cliente> i = c.iterator();
        System.out.println("----------------------------------------------------");
        System.out.println("-- Lista de clientes ordenada por monto facturado --");
        System.out.println("----------------------------------------------------");
        System.out.println("Facturación \t Identidicador \t  \t\t\t Datos Cliente ");
        while (i.hasNext()){
            Cliente cl = i.next();
            System.out.println("\t" + cl.getTotalFacturado() +"\t\t\t " + cl );
        }




    }


}
