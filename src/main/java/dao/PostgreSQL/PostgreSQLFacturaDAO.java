package dao.PostgreSQL;

import csv.CSVcharger;
import dto.Factura;
import factory.PostgreSQLDAOFactory;
import interfaces.InterfaceFacturaDAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PostgreSQLFacturaDAO implements InterfaceFacturaDAO<Factura> {
    public PostgreSQLFacturaDAO() throws Exception {
        if (!PostgreSQLDAOFactory.checkIfExistsEntity("factura", PostgreSQLDAOFactory.conectar())){
            this.crearTabla();
            CSVcharger cargarFacturas = new CSVcharger();
            cargarFacturas.cargarFacturas(this);
        }
    }

    @Override
    public Factura buscar(int id) throws Exception {
        Connection conexion = PostgreSQLDAOFactory.conectar();
        try {
            String query = "SELECT * FROM factura " +
                    "WHERE idFactura = ?";
            PreparedStatement st = conexion.prepareStatement(query);
            st.setInt(1, id);
            ResultSet rs = st.executeQuery();
            if (rs.next())
                return new Factura(rs.getInt(1), rs.getInt(2));
            else
                return null;
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void eliminar(Factura factura) throws Exception {
        Connection conexion = PostgreSQLDAOFactory.conectar();
        try {
            PreparedStatement st = conexion.prepareStatement(
                    "DELETE FROM factura " +
                            "WHERE idFactura = ? ");
            st.setInt(1, factura.getIdFactura());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void registrar(Factura factura) throws Exception {
        Connection conexion = PostgreSQLDAOFactory.conectar();
        try {
            String query =
                    "INSERT INTO factura (idCliente) VALUES (?) ";
            PreparedStatement st = conexion.prepareStatement(query);
            st.setInt(1, factura.getIdCliente());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    @Override
    public void modificar(Factura factura) throws Exception {
        Connection conexion = PostgreSQLDAOFactory.conectar();
        try {
            String query =
                    "UPDATE  factura " +
                            "SET idCliente = ? " +
                            "WHERE  idCliente = ?";
            PreparedStatement st = conexion.prepareStatement(query);
            st.setInt(1, factura.getIdCliente());
            st.setInt(2, factura.getIdFactura());
            st.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            conexion.close();
        }
    }

    public void crearTabla() throws Exception {
        Connection conexion = PostgreSQLDAOFactory.conectar();
        String query =
                "CREATE TABLE IF NOT EXISTS factura (" +
                        "idFactura SERIAL PRIMARY KEY, " +
                        "idCliente INT, " +
                        "FOREIGN KEY (idCliente) REFERENCES cliente(idCliente)" +
                        ")";
        conexion.prepareStatement(query).execute();
        conexion.close();
        System.out.println("Tabla Factura Creada");
    }
}
