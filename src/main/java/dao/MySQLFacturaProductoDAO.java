package dao;

import csv.CSVcharger;
import dto.FacturaProducto;
import factory.MySQLDAOFactory;
import interfaces.InterfaceFacturaProductoDAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQLFacturaProductoDAO implements InterfaceFacturaProductoDAO<FacturaProducto> {
    public MySQLFacturaProductoDAO() throws Exception {
        if (!MySQLDAOFactory.checkIfExistsEntity("factura_producto", MySQLDAOFactory.conectar())){
            this.crearTabla();
            CSVcharger cargarFacturasProductos = new CSVcharger();
            cargarFacturasProductos.cargarFacturasProductos(this);
        }
    }

    @Override
    public FacturaProducto buscar(int id) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            String query = "SELECT * FROM factura_producto " +
                    "WHERE idFactura = ?";
            PreparedStatement st = conexion.prepareStatement(query);
            st.setInt(1, id);
            ResultSet rs = st.executeQuery();
            if (rs.next())
                return new FacturaProducto(rs.getInt(1), rs.getInt(2), rs.getInt(3));
            else
                return null;
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void eliminar(FacturaProducto facturaProducto) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            PreparedStatement st = conexion.prepareStatement(
                    "DELETE FROM factura_producto " +
                            "WHERE idFactura = ?");
            st.setInt(1, facturaProducto.getIdFactura());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void registrar(FacturaProducto facturaProducto) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            PreparedStatement st = conexion.prepareStatement(
                    "INSERT INTO factura_producto (idFactura, idProducto, cantidad) " +
                            "VALUES (?,?,?)");
            st.setInt(1, facturaProducto.getIdFactura());
            st.setInt(2, facturaProducto.getIdProducto());
            st.setInt(3, facturaProducto.getCantidad());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void modificar(FacturaProducto facturaProducto) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            String query =
                    "UPDATE factura_producto " +
                            "SET idFactura = ?, idProducto = ?, cantidad =? " +
                            "WHERE  idFactura = ? ";
            PreparedStatement st = conexion.prepareStatement(query);
            st.setInt(1, facturaProducto.getIdFactura());
            st.setInt(2, facturaProducto.getIdProducto());
            st.setInt(3, facturaProducto.getCantidad());
            st.setInt(3, facturaProducto.getIdFactura());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    public void crearTabla() throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        String query = "CREATE TABLE IF NOT EXISTS factura_producto" +
                "(idFactura INT, " +
                "idProducto INT, " +
                "cantidad INT, " +
                "FOREIGN KEY (idProducto) REFERENCES producto(idProducto), " +
                "FOREIGN KEY (idFactura) REFERENCES factura(idFactura)" +
                ")";
        conexion.prepareStatement(query).execute();
        conexion.close();
        System.out.println("Tabla FacturaProducto Creada");
    }


}
