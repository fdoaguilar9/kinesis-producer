/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.flecharoja.stress;

import java.util.Objects;

/**
 *
 * @author Gerardo Arroyo
 */
public class Payload {
    private String comprobante;

    public Payload(String comprobante) {
        this.comprobante = comprobante;
    }

    public String getComprobante() {
        return comprobante;
    }

    public void setComprobante(String comprobante) {
        this.comprobante = comprobante;
    }

    @Override
    public String toString() {
        return "Payload{" +
                "comprobante='" + comprobante + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Payload payload = (Payload) o;
        return payload.comprobante.equals(this.comprobante);
    }

    @Override
    public int hashCode() {
        return Objects.hash(comprobante);
    }
}
