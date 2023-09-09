package interfaces;

//debería haber una interface para cada entidad
public interface InterfaceProductoDAO<Producto> {

    public Producto buscar(int id) throws Exception;

    public void eliminar(Producto t) throws Exception;

    public void registrar(Producto t) throws Exception;

    public void modificar(Producto t) throws Exception;

    public dto.Producto mayorRecaudacionPorProducto() throws Exception;

    void crearTabla() throws Exception;
}
