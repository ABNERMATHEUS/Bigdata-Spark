package Question4;

import java.io.Serializable;

public class AvgCount implements Serializable {

    private int n; //quantidades
    private double valor; //valor

    public AvgCount() {
    }

    public AvgCount( double valor,int n) {
        this.n = n;
        this.valor = valor;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getValor() {
        return valor;
    }

    public void setValor(double valor) {
        this.valor = valor;
    }

}
