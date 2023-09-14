package dto;

public class Cliente {

    private int idCliente;
    private String nombre;
    private String email;
    private Float totalFacturado;


    public Cliente(Integer id, String nombre, String email){
        this.idCliente = id;
        this.email = email;
        this.nombre = nombre;
    }

    public Cliente( String nombre, String email){
        this.email = email;
        this.nombre = nombre;
    }
    public Float getTotalFacturado() { return totalFacturado; }


    public String getNombre() {
        return nombre;
    }

    public int getIdCliente() {
        return idCliente;
    }

    public String getEmail() {
        return email;
    }
    public void setTotalFacturado(Float totalFacturado) { this.totalFacturado = totalFacturado; }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public void setIdCliente(int idCliente) {
        this.idCliente = idCliente;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @Override
    public String toString() {
        return  "ID: " + this.idCliente + " \t - Nombre: " + this.nombre + "\t - Email: " + this.email + "\t";
    }
}
