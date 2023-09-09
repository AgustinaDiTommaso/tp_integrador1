package dao;

import csv.CSVcharger;
import dto.Producto;
import factory.MySQLDAOFactory;
import interfaces.InterfaceProductoDAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQLProductoDAO implements InterfaceProductoDAO<Producto> {
    public MySQLProductoDAO() throws Exception {
        if(!MySQLDAOFactory.checkIfExistsEntity("producto", MySQLDAOFactory.conectar())){
            this.crearTabla();
            CSVcharger cargarProductos = new CSVcharger();
            cargarProductos.cargarProductos(this);
        }
    }

    @Override
    public Producto buscar(int id) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            String query = "SELECT * FROM producto " +
                    "WHERE idProducto = ?";
            PreparedStatement st = conexion.prepareStatement(query);
            st.setInt(1, id);
            ResultSet rs = st.executeQuery();
            if (rs.next())
                return new Producto(rs.getInt(1), rs.getFloat(3), rs.getString(2));
            else
                return null;
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void eliminar(Producto producto) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            PreparedStatement st = conexion.prepareStatement(
                    "DELETE FROM producto " +
                            "WHERE idProducto =? ");
            st.setInt(1, producto.getIdProducto());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void registrar(Producto producto) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            PreparedStatement st = conexion.prepareStatement(
                    "INSERT INTO producto (nombre, valor) " +
                            "VALUES (?,?) ");
            st.setString(1, producto.getNombre());
            st.setFloat(2, producto.getValor());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void modificar(Producto producto) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            String query =
                    "UPDATE producto " +
                    "SET nombre = ?, valor = ?" +
                    "WHERE  idProducto = ? ";
            PreparedStatement st = conexion.prepareStatement(query);
            st.setString(1, producto.getNombre());
            st.setFloat(2, producto.getValor());
            st.setInt(3, producto.getIdProducto());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    public void crearTabla() throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        String query = "CREATE TABLE IF NOT EXISTS producto" +
                "(idProducto INT AUTO_INCREMENT, " +
                "nombre VARCHAR(500)," +
                "valor FLOAT," +
                "PRIMARY KEY (idProducto))";
        conexion.prepareStatement(query).execute();
        conexion.close();
        System.out.println("Tabla Producto Creada");
    }
    public Producto mayorRecaudacionPorProducto() throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        String query = "SELECT p.idProducto, p.nombre, SUM(fp.cantidad) * p.valor as recaudacion " +
                "FROM factura_producto fp " +
                "JOIN producto p on p.idProducto = fp.idProducto " +
                "GROUP BY p.idProducto " +
                "ORDER BY recaudacion DESC " +
                "LIMIT 1; ";
        PreparedStatement st = conexion.prepareStatement(query);
        ResultSet rs = st.executeQuery();
        if (rs.next()){
            Producto p = this.buscar(rs.getInt(1));
            p.setRecaudacion( rs.getFloat(3));
            conexion.close();
            return p;
        }
        conexion.close();
        return null;
    }
}
