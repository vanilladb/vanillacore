package org.vanilladb.core.sql;

import org.vanilladb.core.util.ByteHelper;

import java.util.*;

/**
 * Vector constant stores multiple fp32 values as a constant
 * This would enable vector processing in VanillaCore
 */
public class VectorConstant extends Constant{
    private List<Float> vec;

    public VectorConstant(int size) {
        Random random = new Random();
        vec = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            vec.add(random.nextFloat());
        }
    }

    public VectorConstant(List<Float> vector) {
        vec = new ArrayList<>(vector.size());
        for (Float element : vector) {
            vec.add(element);
        }
    }

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

    @Override
    public Type getType() {
        return Type.VECTOR;
    }

    @Override
    public Object asJavaVal() {
        return vec;
    }

    public List<Float> copy() {
        return new ArrayList<>(vec);
    }

    @Override
    public byte[] asBytes() {
        // TODO: Unit test
        int bufferSize = this.size();
        byte[] buf = new byte[bufferSize];

        for (int i = 0; i < vec.size(); i++) {
            byte[] floatAsBytes = ByteHelper.toBytes(vec.get(i));
            int offset = i * Float.BYTES;
            System.arraycopy(floatAsBytes, 0, buf, offset, Float.BYTES);
        }
        return buf;
    }

    @Override
    public int size() {
        return Float.BYTES * vec.size();
    }

    @Override
    public Constant castTo(Type type) {
        throw new UnsupportedOperationException("Cannot cast vector");
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
        // TODO: SIMD
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
    public int compareTo(Constant o) {
        throw new UnsupportedOperationException("Vector doesn't support comparison");
    }
}
