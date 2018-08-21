/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.query.planner.opt;

import java.util.ArrayList;

import org.vanilladb.core.query.algebra.Plan;

/**
 * This class contains methods for a valid access path
 */
public class AccessPath {
	private Plan p;
	private int hashCode = 0;
	private ArrayList<Integer> tblUsed = new ArrayList<Integer>();

	public AccessPath (TablePlanner newTp, Plan p) {
		this.p = p;
		this.tblUsed.add(newTp.getId());
		this.hashCode = newTp.hashCode();	
	}
	
	public AccessPath (AccessPath preAp, TablePlanner newTp, Plan p) {
		this.p = p;
		this.tblUsed.addAll(preAp.getTblUsed());
		this.tblUsed.add(newTp.getId());
		this.hashCode = preAp.hashCode() + newTp.hashCode();
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
	
	@ Override
	public int hashCode() {
		return hashCode;
	}
}
