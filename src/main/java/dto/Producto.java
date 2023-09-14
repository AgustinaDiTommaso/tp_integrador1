package dto;

public class Producto {
    private Integer idProducto = null;
    private float valor;
    private String nombre;
    private float recaudacion;

    public float getRecaudacion() {
        return recaudacion;
    }

    public void setRecaudacion(float recaudacion) {
        this.recaudacion = recaudacion;
    }

    public Producto(int idProducto, float valor, String nombre) {
        this.idProducto = idProducto;
        this.valor = valor;
        this.nombre = nombre;
    }

    public Producto( float valor, String nombre) {
        this.valor = valor;
        this.nombre = nombre;
    }

    public String getNombre() {
        return nombre;
    }

    public float getValor() {
        return valor;
    }

    public int getIdProducto() {
        return idProducto;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public void setIdProducto(int idProducto) {
        this.idProducto = idProducto;
    }

    public void setValor(float valor) {
        this.valor = valor;
    }

    @Override
    public String toString() {
        return this.idProducto + "\t " + this.nombre  + "\t $" + this.valor;
    }

}
