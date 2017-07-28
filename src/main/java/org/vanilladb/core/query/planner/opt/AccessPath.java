package org.vanilladb.core.query.planner.opt;

import java.util.ArrayList;

import org.vanilladb.core.query.algebra.Plan;

/**
 * This class contains methods for a valid access path
 */
public class AccessPath {
	private Plan p;
	private int binaryCode = 0;
	private ArrayList<Integer> tblUsed = new ArrayList<Integer>();

	public AccessPath (TablePlanner newTp, Plan p) {
		this.p = p;
		this.tblUsed.add(newTp.getTblNum());
		this.binaryCode = newTp.getBinaryCode();	
	}
	
	public AccessPath (AccessPath preAp, TablePlanner newTp, Plan p) {
		this.p = p;
		this.tblUsed.addAll(preAp.getTblUsed());
		this.tblUsed.add(newTp.getTblNum());
		this.binaryCode = preAp.getBinaryCode() + newTp.getBinaryCode();
	}
	
	public Plan getPlan () {
		return p;
	}
	
	public long getCost () {
		return p.recordsOutput();
	}
	
	public ArrayList<Integer> getTblUsed () {
		return tblUsed;
	}
	
	public boolean isUsed (int tbl) {
		return tblUsed.contains(tbl);
	}

	public int getBinaryCode() {
		return binaryCode;
	}
}
