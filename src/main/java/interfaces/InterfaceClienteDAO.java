package interfaces;

import dto.Cliente;

import java.util.ArrayList;

//debería haber una interface para cada entidad
public interface InterfaceClienteDAO<Cliente> {

    public Cliente buscar(int id) throws Exception;

    public void eliminar(Cliente t) throws Exception;

    public void registrar(Cliente t) throws Exception;

    public void modificar(Cliente t) throws Exception;

    void crearTabla() throws Exception;

    public ArrayList<Cliente> obtenerClientePorRecaudacion() throws Exception;
}
