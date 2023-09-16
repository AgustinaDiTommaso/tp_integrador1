package dao.MongoDB;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import csv.CSVcharger;
import dto.FacturaProducto;
import factory.MongoDBDAOFactory;
import factory.MySQLDAOFactory;
import interfaces.InterfaceFacturaProductoDAO;
import org.bson.Document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MongoDBFacturaProductoDAO implements InterfaceFacturaProductoDAO<FacturaProducto> {
    public MongoDBFacturaProductoDAO() throws Exception {
        if (!MongoDBDAOFactory.checkIfExistsEntity("factura_producto", MongoDBDAOFactory.conectar())){
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

        try {
            MongoDatabase db = MongoDBDAOFactory.conectar();
            Document documentoFacturaProducto =
                    new Document(
                            "idFactura", facturaProducto.getIdFactura())
                            .append("idProducto", facturaProducto.getIdProducto())
                            .append("cantidad", facturaProducto.getCantidad()
                            );
            MongoCollection<Document> coleccionFacturaProductos = db.getCollection("factura_producto");
            coleccionFacturaProductos.insertOne(documentoFacturaProducto);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
        MongoDatabase db = MongoDBDAOFactory.conectar();
        db.createCollection("factura_producto");
        System.out.println("Tabla FacturaProducto Creada");
    }


}
