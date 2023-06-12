package org.vanilladb.core.query.parse;

import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;

public class VectorEmbeddingData {
    private final VectorConstant query;
    private final String embField;
    private final DistanceFn distFn;

    public VectorEmbeddingData(VectorConstant query, String embField, DistanceFn distFn) {
        this.query = query;
        this.embField = embField;
        this.distFn = distFn;
    }

    public VectorConstant getQueryVector() {
        return query;
    }

    public String getEmbeddingField() {
        return embField;
    }

    public DistanceFn getDistanceFn() {
        return distFn;
    }

    public double distance(VectorConstant v) {
        return distFn.distance(query, v);
    }
}
