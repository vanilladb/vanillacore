package org.vanilladb.core.query.parse;

import org.vanilladb.core.sql.VectorConstant;

public class VectorQueryData {
    // TODO: Builder pattern
    private final VectorConstant query;
    private final boolean approximate;

    private final String collectionName;
    private final String embField;
    private boolean isExplain;

    public VectorQueryData(VectorConstant query, String collectionName, String embField, boolean approximate) {
        this.query = query; // TODO: Create a copy
        this.collectionName = collectionName;
        this.embField = embField;
        this.approximate = approximate;
    }

    public boolean isApproximate() {
        return approximate;
    }

    public VectorConstant getVector() {
        return query; // TODO: Create a copy
    }

    public String getCollectionName() {
        return collectionName;
    }

    public boolean isExplain() {
        return isExplain;
    }

    public String getEmbeddingField() {
        return this.embField;
    }
}
