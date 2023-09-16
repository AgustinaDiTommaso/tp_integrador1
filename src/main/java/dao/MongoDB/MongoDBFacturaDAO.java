package dao.MongoDB;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import csv.CSVcharger;
import dto.Factura;
import factory.MongoDBDAOFactory;
import factory.MySQLDAOFactory;
import interfaces.InterfaceFacturaDAO;
import org.bson.Document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MongoDBFacturaDAO implements InterfaceFacturaDAO<Factura> {
    public MongoDBFacturaDAO() throws Exception {
        if (!MongoDBDAOFactory.checkIfExistsEntity("factura", MongoDBDAOFactory.conectar())) {
            this.crearTabla();
            CSVcharger cargarFacturas = new CSVcharger();
            cargarFacturas.cargarFacturas(this);
        }
    }

    @Override
    public Factura buscar(int id) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
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
        Connection conexion = MySQLDAOFactory.conectar();
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
        MongoDatabase db = MongoDBDAOFactory.conectar();
        Document documentoFactura =
                new Document("idCliente", factura.getIdFactura());
        MongoCollection<Document> coleccionFactura = db.getCollection("factura");
        coleccionFactura.insertOne(documentoFactura);
    }

    @Override
    public void modificar(Factura factura) throws Exception {
        Connection conexion = MySQLDAOFactory.conectar();
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
        MongoDatabase db = MongoDBDAOFactory.conectar();
        db.createCollection("factura");
        System.out.println("Tabla Factura Creada");
    }
}
