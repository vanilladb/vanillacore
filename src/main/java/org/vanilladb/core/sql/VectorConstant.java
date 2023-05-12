package org.vanilladb.core.sql;

import org.vanilladb.core.util.ByteHelper;

import java.util.*;

/**
 * Vector constant stores multiple fp32 values as a constant
 * This would enable vector processing in VanillaCore
 */
public class VectorConstant extends Constant {
    // TODO: Use primitive type
    private List<Float> vec;

    /**
     * Return a vector constant with random values
     * @param size size of the vector
     */
    public VectorConstant(int size) {
        Random random = new Random();
        vec = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            vec.add(random.nextFloat());
        }
    }

    /**
     * Return a vector constant with given values
     * @param vector values of the vector
     */
    public VectorConstant(List<Float> vector) {
        vec = new ArrayList<>(vector.size());
        for (Float element : vector) {
            vec.add(element);
        }
    }

    /**
     * Reconstruct a vector constant from bytes
     * @param bytes bytes to reconstruct
     */
    public VectorConstant(byte[] bytes) {
        int size = bytes.length / Float.BYTES;
        vec = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            byte[] floatAsBytes = new byte[Float.BYTES];
            int offset = i * Float.BYTES;
            System.arraycopy(bytes, offset, floatAsBytes, 0, Float.BYTES);
            vec.add(ByteHelper.toFloat(floatAsBytes));
        }
    }

    /**
     * Return the type of the constant
     */
    @Override
    public Type getType() {
        return Type.VECTOR;
    }

    /**
     * Return the value of the constant
     */
    @Override
    public Object asJavaVal() {
        return vec;
    }

    /**
     * Return a copy of the vector
     * @return
     */
    public List<Float> copy() {
        return new ArrayList<>(vec);
    }


    /** 
     * Return the vector as bytes
    */
    @Override
    public byte[] asBytes() {
        int bufferSize = this.size();
        byte[] buf = new byte[bufferSize];

        for (int i = 0; i < vec.size(); i++) {
            byte[] floatAsBytes = ByteHelper.toBytes(vec.get(i));
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
        return Float.BYTES * vec.size();
    }

    /**
     * Return the size of the vector
     * @return size of the vector
     */
    public int length() {
        return vec.size();
    }

    @Override
    public Constant castTo(Type type) {
        if (getType().equals(type))
            return this;
        throw new IllegalArgumentException("Cannot cast vector to other types");
    }

    public float get(int idx) {
        return vec.get(idx);
    }

    @Override
    public Constant add(Constant c) {
        if (!(c instanceof VectorConstant))
            throw new UnsupportedOperationException("Vector doesn't support single value addition");

        if (this.size() != c.size())
            throw new ArithmeticException("Vectors are not the same size");

        List<Float> result = ((VectorConstant) c).copy();

        for (int i = 0; i < vec.size(); i++) {
            result.set(i, vec.get(i) + result.get(i));
        }

        return new VectorConstant(result);
    }

    @Override
    public Constant sub(Constant c) {
        if (!(c instanceof VectorConstant))
            throw new UnsupportedOperationException("Vector doesn't support single value subtraction");

        if (this.size() != c.size())
            throw new ArithmeticException("Vectors are not the same size");

        List<Float> result = ((VectorConstant) c).copy();
        for (int i = 0; i < vec.size(); i++) {
            result.set(i, vec.get(i) - result.get(i));
        }

        return new VectorConstant(result);
    }

    @Override
    public Constant mul(Constant c) {
        if (!(c instanceof VectorConstant))
            throw new UnsupportedOperationException("Vector doesn't support single value multiplication");

        if (this.size() != c.size())
            throw new ArithmeticException("Vectors are not the same size");

        List<Float> result = ((VectorConstant) c).copy();
        for (int i = 0; i < vec.size(); i++) {
            result.set(i, vec.get(i) * result.get(i));
        }

        return new VectorConstant(result);
    }

    @Override
    public Constant div(Constant c) {
        throw new UnsupportedOperationException("Vector doesn't support division");
    }

    @Override
    public int compareTo(Constant c) {
        throw new UnsupportedOperationException("Vector does not support comparison with other types");
    }

    public boolean equals(VectorConstant o) {
        if (o.size() != this.size())
            return false;

        for (int i = 0; i < this.length(); i++) {
            if (vec.get(i) != o.get(i))
                return false;
        }
        return true;
    }
}
