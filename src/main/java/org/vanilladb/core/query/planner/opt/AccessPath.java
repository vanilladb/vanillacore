package org.vanilladb.core.query.planner.opt;

import java.util.ArrayList;

import org.vanilladb.core.query.algebra.Plan;

public class AccessPath {
	private Plan trunk;
	private int hashCode = 0;
	private ArrayList<TablePlanner> combination = new ArrayList<TablePlanner>();
	
	// add new member of current layer to combination and plus the hash code
	public void add(int layer, TablePlanner tp) {
		if (combination.size() == layer) 
			combination.add(tp);
		else
			combination.set(layer, tp);
		hashCode += Math.pow(2, tp.getIndexNum());
	}
	
	// when jump to previous layer, need to subtract the hash code of current layer
	public void del(TablePlanner tp) {
		hashCode -= Math.pow(2, tp.getIndexNum());
	}
	
	// get the ith table planner of combination
	public TablePlanner getTablePlanner (int index) {
		return combination.get(index);
	}
	
	// method for view
	public void setTrunk (Plan p) {
		trunk = p;
	}
	
	// method for view
	public Plan getTrunk () {
		return trunk;
	}
	
	@Override
	public int hashCode() {
		return hashCode;
	}
}
