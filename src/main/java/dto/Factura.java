package dto;

public class Factura {
    private int idFactura;
    private int idCliente;
    public Factura(int idF, int idC){
        this.idFactura = idF;
        this.idCliente = idC;
    }

    public Factura(int idC){
        this.idCliente = idC;
    }

    public int getIdCliente() {
        return idCliente;
    }

    public int getIdFactura() {
        return idFactura;
    }

    public void setIdCliente(int idCliente) {
        this.idCliente = idCliente;
    }

    public void setIdFactura(int idFactura) {
        this.idFactura = idFactura;
    }
}
