package org.vanilladb.core.sql;


public class VectorConstantRange extends ConstantRange {

    VectorConstant val;

    public VectorConstantRange(VectorConstant arg) {
        this.val = arg;
    }

    @Override
    public boolean isValid() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isValid'");
    }

    @Override
    public boolean hasLowerBound() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'hasLowerBound'");
    }

    @Override
    public boolean hasUpperBound() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'hasUpperBound'");
    }

    @Override
    public Constant low() {
        return val;
    }

    @Override
    public Constant high() {
        return val;
    }

    @Override
    public boolean isLowInclusive() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isLowInclusive'");
    }

    @Override
    public boolean isHighInclusive() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isHighInclusive'");
    }

    @Override
    public double length() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'length'");
    }

    @Override
    public ConstantRange applyLow(Constant c, boolean inclusive) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'applyLow'");
    }

    @Override
    public ConstantRange applyHigh(Constant c, boolean incl) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'applyHigh'");
    }

    @Override
    public ConstantRange applyConstant(Constant c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'applyConstant'");
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public Constant asConstant() {
        return val;
    }

    @Override
    public boolean contains(Constant c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'contains'");
    }

    @Override
    public boolean lessThan(Constant c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'lessThan'");
    }

    @Override
    public boolean largerThan(Constant c) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'largerThan'");
    }

    @Override
    public boolean isOverlapping(ConstantRange r) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isOverlapping'");
    }

    @Override
    public boolean contains(ConstantRange r) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'contains'");
    }

    @Override
    public ConstantRange intersect(ConstantRange r) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'intersect'");
    }

    @Override
    public ConstantRange union(ConstantRange r) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'union'");
    }

}