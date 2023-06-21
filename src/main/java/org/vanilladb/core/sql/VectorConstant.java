package org.vanilladb.core.sql;

import static java.sql.Types.VARCHAR;

import java.io.Serializable;

import org.vanilladb.core.util.ByteHelper;

import java.util.*;

/**
 * Vector constant stores multiple float32 values as a constant
 * This would enable vector processing in VanillaCore
 */
public class VectorConstant extends Constant implements Serializable {
    private float[] vec;
    private Type type;

    public static VectorConstant zeros(int dimension) {
        float[] vec = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vec[i] = 0;
        }
        return new VectorConstant(vec);
    }

    public static VectorConstant normal(int dimension, float mean, float std) {
        float[] vec = new float[dimension];
        Random rvg = new Random();
        for (int i = 0; i < dimension; i++) {
            vec[i] = Math.round((float) rvg.nextGaussian(mean, std) * 100) / 100.0f;
        }
        return new VectorConstant(vec);
    }

    /**
     * Return a vector constant with random values
     * @param length size of the vector
     */
    public VectorConstant(int length) {
        type = new VectorType(length);
        Random random = new Random();
        vec = new float[length];
        for (int i = 0; i < length; i++) {
            vec[i] = random.nextInt(9999);
        }
    }

    public VectorConstant(float[] vector) {
        type = new VectorType(vector.length);
        vec = new float[vector.length];
        
        for (int i = 0; i < vector.length; i++) {
            vec[i] = vector[i];
        }
    }

    public VectorConstant(List<Float> vector) {
        int length = vector.size();
        
        type = new VectorType(length);
        vec = new float[length];
        
        for (int i = 0; i < length; i++) {
            vec[i] = vector.get(i);
        }
    }

    public VectorConstant(VectorConstant v) {
        vec = new float[v.dimension()];
        int i = 0;
        for (float e : v.asJavaVal()) {
            vec[i++] = e;
        }
        type = new VectorType(v.dimension());
    }

    public VectorConstant(String vectorString) {
        String[] split = vectorString.split(" ");

        type = new VectorType(split.length);
        vec = new float[split.length];

        for (int i = 0; i < split.length; i++) {
            vec[i] = Float.valueOf(split[i]);
        }
    }

    /**
     * Reconstruct a vector constant from bytes
     * @param bytes bytes to reconstruct
     */
    public VectorConstant(byte[] bytes) {
        int length = bytes.length / Float.BYTES;
        type = new VectorType(length);
        // vec = new ArrayList<>(length);
        vec = new float[length];
        for (int i = 0; i < length; i++) {
            byte[] floatAsBytes = new byte[Float.BYTES];
            int offset = i * Float.BYTES;
            System.arraycopy(bytes, offset, floatAsBytes, 0, Float.BYTES);
            vec[i] = ByteHelper.toFloat(floatAsBytes);
        }
    }

    /**
     * Return the type of the constant
     */
    @Override
    public Type getType() {
        return type;
    }

    /**
     * Return the value of the constant
     */
    @Override
    public float[] asJavaVal() {
        return vec;
    }

    /**
     * Return a copy of the vector
     * @return
     */
    public float[] copy() {
        return Arrays.copyOf(vec, vec.length);
    }


    /** 
     * Return the vector as bytes
    */
    @Override
    public byte[] asBytes() {
        int bufferSize = this.size();
        byte[] buf = new byte[bufferSize];

        for (int i = 0; i < vec.length; i++) {
            byte[] floatAsBytes = ByteHelper.toBytes(vec[i]);
            int offset = i * Float.BYTES;
            System.arraycopy(floatAsBytes, 0, buf, offset, Float.BYTES);
        }
        return buf;
    }

    /**
     * Return the size of the vector in bytes
     */
    @Override
    public int size() {
        return Float.BYTES * vec.length;
    }

    /**
     * Return the size of the vector
     * @return size of the vector
     */
    public int dimension() {
        return vec.length;
    }

    @Override
    public Constant castTo(Type type) {
        if (getType().equals(type))
            return this;
        switch (type.getSqlType()) {
            case VARCHAR:
                return new VarcharConstant(toString(), type);
            }
        throw new IllegalArgumentException("Cannot cast vector to " + type);
    }

    public float get(int idx) {
        return vec[idx];
    }

    @Override
    public Constant add(Constant c) {
        if (!(c instanceof VectorConstant)) {
            throw new UnsupportedOperationException("Vector doesn't support addition with other constants");
        }

        assert dimension() == ((VectorConstant) c).dimension();

        float[] res = new float[dimension()];
        for (int i = 0; i < dimension(); i++) {
            res[i] = this.get(i) + ((VectorConstant) c).get(i);
        }
        return new VectorConstant(res);
    }

    @Override
    public Constant sub(Constant c) {
        if (!(c instanceof VectorConstant)) {
            throw new UnsupportedOperationException("Vector doesn't support subtraction with other constants");
        }

        assert dimension() == ((VectorConstant) c).dimension();

        float[] res = new float[dimension()];
        for (int i = 0; i < dimension(); i++) {
            res[i] = this.get(i) - ((VectorConstant) c).get(i);
        }
        return new VectorConstant(res);
    }

    @Override
    public Constant mul(Constant c) {
        throw new UnsupportedOperationException("Vector doesn't support multiplication");
    }

    @Override
    public Constant div(Constant c) {
        if (!(c instanceof IntegerConstant)) {
            throw new UnsupportedOperationException("Vector doesn't support subtraction with other constants");
        }
        float[] res = new float[dimension()];
        for (int i = 0; i < dimension(); i++) {
            res[i] = this.get(i) / (Integer) c.asJavaVal();
        }
        return new VectorConstant(res);
    }

    @Override
    public int compareTo(Constant c) {
        // if (!(c instanceof VectorConstant))
        //     throw new IllegalArgumentException("Vector does not support comparison with other types");
        // VectorConstant o = (VectorConstant) c;
        throw new IllegalArgumentException("VectorConstant does not support comparison");
    }

    public boolean equals(VectorConstant o) {
        if (o.size() != this.size())
            return false;

        for (int i = 0; i < dimension(); i++) {
            if (vec[i] != o.get(i))
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(vec);
    }

    public int[] hashCode(int bands, int buckets) {
        assert dimension() % bands == 0;

        int chunkSize = dimension() / bands;

        int[] hashCodes = new int[bands];
        for (int i = 0; i < bands; i++) {
            int hashCode = (Arrays.hashCode(Arrays.copyOfRange(vec, i * chunkSize, (i + 1) * chunkSize))) % buckets;
            if (hashCode < 0)
                hashCode += buckets;
            hashCodes[i] = hashCode;
        }
        return hashCodes;
    }
}
