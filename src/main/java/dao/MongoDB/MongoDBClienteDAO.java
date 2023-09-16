package dao.MongoDB;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import csv.CSVcharger;
import dto.Cliente;
import factory.MongoDBDAOFactory;
import factory.MySQLDAOFactory;
import interfaces.InterfaceClienteDAO;
import org.bson.Document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

public class MongoDBClienteDAO implements InterfaceClienteDAO<Cliente> {
    public MongoDBClienteDAO() throws Exception {
        if(!MongoDBDAOFactory.checkIfExistsEntity("cliente", MongoDBDAOFactory.conectar())){
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
        MongoDatabase db = MongoDBDAOFactory.conectar();
        Document documentoCliente =
                new Document(
                        "nombre", cliente.getNombre())
                        .append("mail", cliente.getEmail()
                        );
        MongoCollection<Document> coleccionCliente = db.getCollection("cliente");
        coleccionCliente.insertOne(documentoCliente);
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
        MongoDatabase db = MongoDBDAOFactory.conectar();
        db.createCollection("cliente");
        System.out.println("Tabla Cliente Creada");
    }

    @Override
    public ArrayList<Cliente> obtenerClientePorRecaudacion() throws Exception {


        // Define la etapa de agregación
        Document lookupProductos = new Document("$lookup", new Document("from", "producto")
                .append("localField", "idFactura")
                .append("foreignField", "idFactura")
                .append("as", "productos"));

        Document unwindProductos = new Document("$unwind", "$productos");

        Document lookupCliente = new Document("$lookup", new Document("from", "cliente")
                .append("localField", "idCliente")
                .append("foreignField", "idCliente")
                .append("as", "cliente"));

        Document unwindCliente = new Document("$unwind", "$cliente");

        Document group = new Document("$group", new Document("_id", new Document("idCliente", "$cliente.idCliente")
                .append("nombre", "$cliente.nombre")
                .append("email", "$cliente.email"))
                .append("total_valor", new Document("$sum", new Document("$multiply", ArrayList.asList("$productos.cantidad", "$productos.valor")))));

        Document project = new Document("$project", new Document("_id", 0)
                .append("total_valor", 1)
                .append("idCliente", "$_id.idCliente")
                .append("nombre", "$_id.nombre")
                .append("email", "$_id.email"));

        Document sort = new Document("$sort", new Document("total_valor", -1));

        // Ejecuta la agregación
        Iterable<Document> results = facturaCollection.aggregate(
                Arrays.asList(lookupProductos, unwindProductos, lookupCliente, unwindCliente, group, project, sort)
        );


        Connection conexion = MySQLDAOFactory.conectar();
        String query =  "SELECT  SUM(fp.cantidad * p.valor) AS total_valor, f.idCliente, c.nombre, c.email " +
                        "FROM factura_producto fp " +
                        "JOIN producto p ON p.idProducto = fp.idProducto " +
                        "JOIN factura f ON f.idFactura = fp.idFactura " +
                        "JOIN cliente c ON c.idCliente = f.idCliente " +
                        "GROUP BY f.idCliente , c.nombre, c.email " +
                        "order by total_valor DESC";
        PreparedStatement st = conexion.prepareStatement(query);
        ResultSet rs = st.executeQuery();

        ArrayList<Cliente> clientes = new ArrayList<>();
        while (rs.next()){
            Cliente cliente = new Cliente(rs.getInt(2), rs.getString(3), rs.getString(4));
            cliente.setTotalFacturado( rs.getFloat(1));
            clientes.add(cliente);
        }

        conexion.close();
        return clientes;
    }
}
