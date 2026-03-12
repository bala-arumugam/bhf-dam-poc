package com.bhf.dam.poc.core.beans;

public class ColumnDefinition {

    private final String header;
    private final String propertyPath;

    public ColumnDefinition(String header, String propertyPath) {
        this.header = header;
        this.propertyPath = propertyPath;
    }

    public String getHeader() {
        return header;
    }

    public String getPropertyPath() {
        return propertyPath;
    }
}
