package org.vanilladb.core.sql;

import static java.sql.Types.VARCHAR;

import org.vanilladb.core.util.ByteHelper;

import java.util.*;

/**
 * Vector constant stores multiple float32 values as a constant
 * This would enable vector processing in VanillaCore
 */
public class VectorConstant extends Constant {
    private float[] vec;
    private Type type;

    public static VectorConstant zeros(int length) {
        float[] vec = new float[length];
        for (int i = 0; i < length; i++) {
            vec[i] = 0.0f;
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
            vec[i] = random.nextFloat();
        }
    }

    public VectorConstant(float[] vector) {
        type = new VectorType(vector.length);
        vec = new float[vector.length];
        
        for (int i = 0; i < vector.length; i++) {
            vec[i] = vector[i];
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
    public int length() {
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
        if (!(c instanceof VectorConstant))
            throw new UnsupportedOperationException("Vector doesn't support single value addition");

        if (this.size() != c.size())
            throw new ArithmeticException("Vectors are not the same size");

        float[] result = ((VectorConstant) c).copy();

        for (int i = 0; i < vec.length; i++) {
            result[i] = vec[i] + result[i];
        }

        return new VectorConstant(result);
    }

    @Override
    public Constant sub(Constant c) {
        if (!(c instanceof VectorConstant))
            throw new UnsupportedOperationException("Vector doesn't support single value subtraction");

        if (this.size() != c.size())
            throw new ArithmeticException("Vectors are not the same size");

        float[] result = ((VectorConstant) c).copy();
        for (int i = 0; i < vec.length; i++) {
            result[i] = vec[i] - result[i];
        }

        return new VectorConstant(result);
    }

    @Override
    public Constant mul(Constant c) {
        if (!(c instanceof VectorConstant))
            throw new UnsupportedOperationException("Vector doesn't support single value multiplication");

        if (this.size() != c.size())
            throw new ArithmeticException("Vectors are not the same size");

        float[] result = ((VectorConstant) c).copy();
        for (int i = 0; i < vec.length; i++) {
            result[i] = vec[i] * result[i];
        }

        return new VectorConstant(result);
    }

    @Override
    public Constant div(Constant c) {
        throw new UnsupportedOperationException("Vector doesn't support division");
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

        for (int i = 0; i < this.length(); i++) {
            if (vec[i] != o.get(i))
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(vec);
    }
}
