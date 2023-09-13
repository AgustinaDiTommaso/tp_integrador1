package dao;

import csv.CSVcharger;
import dto.Cliente;
import factory.MySQLDAOFactory;
import interfaces.InterfaceClienteDAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

public class MySQLClienteDAO implements InterfaceClienteDAO<Cliente> {
    public MySQLClienteDAO() throws Exception {
        if(!MySQLDAOFactory.checkIfExistsEntity("cliente", MySQLDAOFactory.conectar())){
            this.crearTabla();
            CSVcharger cargarClientes = new CSVcharger<>();
            cargarClientes.cargaClientes(this);
        }
    }

    @Override
    public Cliente buscar(int id) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            String query = "SELECT * FROM cliente " +
                    "WHERE idCliente = ?";
            PreparedStatement st = conexion.prepareStatement(query);
            st.setInt(1, id);
            ResultSet rs = st.executeQuery();
            if (rs.next())
                return new Cliente(rs.getInt(1), rs.getString(2), rs.getString(3));
            else
                return null;
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void eliminar(Cliente cliente) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            PreparedStatement st = conexion.prepareStatement(
                    "DELETE FROM cliente " +
                            "WHERE idCliente =? ");
            st.setInt(1, cliente.getIdCliente());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void registrar(Cliente cliente) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            PreparedStatement st = conexion.prepareStatement(
                    "INSERT INTO cliente (nombre, email) " +
                            "VALUES (?,?) ");
            st.setString(1, cliente.getNombre());
            st.setString(2, cliente.getEmail());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void modificar(Cliente cliente) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        try {
            String query =
                    "UPDATE  cliente " +
                    "SET nombre = ?, email = ?" +
                    "WHERE idCliente = ?";
            PreparedStatement st = conexion.prepareStatement(query);
            st.setString(1, cliente.getNombre());
            st.setString(2, cliente.getEmail());
            st.setInt(3, cliente.getIdCliente());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    public void crearTabla() throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        String query = "CREATE TABLE IF NOT EXISTS cliente" +
                "(idCliente INT AUTO_INCREMENT, " +
                "nombre VARCHAR(500)," +
                "email VARCHAR(150)," +
                "PRIMARY KEY (idCliente))";
        conexion.prepareStatement(query).execute();
        conexion.close();
        System.out.println("Tabla Cliente Creada");
    }

    @Override
    public ArrayList<Cliente> obtenerClientePorRecaudacion() throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
        String query = "SELECT f.idCliente, c.nombre, SUM(fp.cantidad * p.valor) AS total_valor " +
                "FROM factura_producto fp " +
                "JOIN producto p ON p.idProducto = fp.idProducto " +
                "JOIN factura f ON f.idFactura = fp.idFactura " +
                "JOIN cliente c ON c.idCliente = f.idCliente " +
                "GROUP BY f.idCliente , c.nombre " +
                "order by total_valor DESC";
        PreparedStatement st = conexion.prepareStatement(query);
        ResultSet rs = st.executeQuery();

        ArrayList<Cliente> clientes = new ArrayList<>();
        Cliente cliente;
        while (rs.next()){
            cliente = this.buscar(rs.getInt(1));
            cliente.setTotalFacturado( rs.getFloat(3));
            clientes.add(cliente);
        }
        conexion.close();
        return clientes;
    }
}
