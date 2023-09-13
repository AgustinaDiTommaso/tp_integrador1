package interfaces;

//debería haber una interface para cada entidad
public interface InterfaceFacturaDAO<T> {

    public T buscar(int id) throws Exception;

    public void eliminar(T t) throws Exception;

    public void registrar(T t) throws Exception;

    public void modificar(T t) throws Exception;

    void crearTabla() throws Exception;
}
