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
package org.vanilladb.core.query.parse;

import static org.vanilladb.core.sql.RecordComparator.DIR_ASC;
import static org.vanilladb.core.sql.RecordComparator.DIR_DESC;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.DOUBLE;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.sql.predicate.BinaryArithmeticExpression.OP_ADD;
import static org.vanilladb.core.sql.predicate.BinaryArithmeticExpression.OP_DIV;
import static org.vanilladb.core.sql.predicate.BinaryArithmeticExpression.OP_MUL;
import static org.vanilladb.core.sql.predicate.BinaryArithmeticExpression.OP_SUB;
import static org.vanilladb.core.sql.predicate.Term.OP_EQ;
import static org.vanilladb.core.sql.predicate.Term.OP_GT;
import static org.vanilladb.core.sql.predicate.Term.OP_GTE;
import static org.vanilladb.core.sql.predicate.Term.OP_LT;
import static org.vanilladb.core.sql.predicate.Term.OP_LTE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.aggfn.AggregationFn;
import org.vanilladb.core.sql.aggfn.AvgFn;
import org.vanilladb.core.sql.aggfn.CountFn;
import org.vanilladb.core.sql.aggfn.DistinctCountFn;
import org.vanilladb.core.sql.aggfn.MaxFn;
import org.vanilladb.core.sql.aggfn.MinFn;
import org.vanilladb.core.sql.aggfn.SumFn;
import org.vanilladb.core.sql.predicate.BinaryArithmeticExpression;
import org.vanilladb.core.sql.predicate.ConstantExpression;
import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.FieldNameExpression;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.util.CoreProperties;

/**
 * The VanillaDb parser. Note that VanillaDb parser cannot parse double value in
 * scientific notation.
 */
public class Parser {
	public static final IndexType DEFAULT_INDEX_TYPE;

	static {
		int defaultIdxType = CoreProperties.getLoader().getPropertyAsInteger(
				Parser.class.getName() + ".DEFAULT_INDEX_TYPE", 1);
		DEFAULT_INDEX_TYPE = IndexType.fromInteger(defaultIdxType);
	}

	private static class ProjectEl {
		String fld;
		AggregationFn aggFn;

		ProjectEl(String fld) {
			this.fld = fld;
		}

		ProjectEl(AggregationFn aggFn) {
			this.aggFn = aggFn;
		}
	}

	private static class ProjectList {
		List<ProjectEl> els = new ArrayList<ProjectEl>();

		void addField(String fld) {
			els.add(new ProjectEl(fld));
		}

		void addAggFn(AggregationFn aggFn) {
			els.add(new ProjectEl(aggFn));
		}

		Set<String> asStringSet() {
			if (els.size() == 0)
				return null;
			Set<String> ret = new HashSet<String>(els.size());
			for (ProjectEl el : els) {
				if (el.fld != null)
					ret.add(el.fld);
				else
					ret.add(el.aggFn.fieldName());
			}
			return ret;
		}

		Set<AggregationFn> aggregationFns() {
			if (els.size() == 0)
				return null;
			Set<AggregationFn> ret = new HashSet<AggregationFn>(els.size());
			for (ProjectEl el : els) {
				if (el.aggFn != null)
					ret.add(el.aggFn);
			}
			return ret.size() == 0 ? null : ret;
		}
	}

	private static class SortEl extends ProjectEl {
		int dir;

		SortEl(String fld, int dir) {
			super(fld);
			this.dir = dir;
		}

		SortEl(AggregationFn aggFn, int dir) {
			super(aggFn);
			this.dir = dir;
		}
	}

	private static class SortList {
		List<SortEl> els = new ArrayList<SortEl>();

		void addField(String fld, int sortDir) {
			els.add(new SortEl(fld, sortDir));
		}

		void addAggFn(AggregationFn aggFn, int sortDir) {
			els.add(new SortEl(aggFn, sortDir));
		}

		List<String> fieldList() {
			if (els.size() == 0)
				return null;
			List<String> ret = new ArrayList<String>(els.size());
			for (SortEl el : els) {
				if (el.fld != null)
					ret.add(el.fld);
				else
					ret.add(el.aggFn.fieldName());
			}
			return ret;
		}

		List<Integer> directionList() {
			if (els.size() == 0)
				return null;
			List<Integer> ret = new ArrayList<Integer>(els.size());
			for (SortEl el : els)
				ret.add(el.dir);
			return ret;
		}
	}

	/*
	 * Instance members.
	 */

	private Lexer lex;

	public Parser(String s) {
		lex = new Lexer(s);
	}

	/*
	 * Methods for parsing constants and IDs.
	 */

	private String id() {
		return lex.eatId();
	}

	private Constant constant() {
		if (lex.matchStringConstant())
			return new VarcharConstant(lex.eatStringConstant());
		else
			return new DoubleConstant(lex.eatNumericConstant());
	}

	private Set<String> idSet() {
		Set<String> list = new HashSet<String>();
		do {
			if (lex.matchDelim(','))
				lex.eatDelim(',');
			list.add(id());
		} while (lex.matchDelim(','));
		return list;
	}

	private List<String> idList() {
		List<String> list = new ArrayList<String>();
		do {
			if (lex.matchDelim(','))
				lex.eatDelim(',');
			list.add(id());
		} while (lex.matchDelim(','));
		return list;
	}

	private List<Constant> constList() {
		List<Constant> list = new ArrayList<Constant>();
		do {
			if (lex.matchDelim(','))
				lex.eatDelim(',');
			list.add(constant());
		} while (lex.matchDelim(','));
		return list;
	}

	/*
	 * Methods for parsing queries.
	 */
	public QueryData queryCommand() {
		boolean isExplain = false;
		if (lex.matchKeyword("explain")) {
			isExplain = true;
			lex.eatKeyword("explain");
		}
		lex.eatKeyword("select");
		ProjectList projs = projectList();
		lex.eatKeyword("from");
		Set<String> tables = idSet();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		/*
		 * Non-null group-by fields (but may be empty) if "group by" appears or
		 * there is an aggFn in the project list.
		 */
		Set<String> groupFields = null;
		if (lex.matchKeyword("group")) {
			lex.eatKeyword("group");
			lex.eatKeyword("by");
			groupFields = idSet();
		}
		if (groupFields == null && projs.aggregationFns() != null)
			groupFields = new HashSet<String>();
		// Need to preserve the order of sort fields
		List<String> sortFields = null;
		List<Integer> sortDirs = null;
		if (lex.matchKeyword("order")) {
			lex.eatKeyword("order");
			lex.eatKeyword("by");
			// neither null nor empty if "sort by" appears
			SortList sortList = sortList();
			sortFields = sortList.fieldList();
			sortDirs = sortList.directionList();
		}
		return new QueryData(isExplain, projs.asStringSet(), tables, pred,
				groupFields, projs.aggregationFns(), sortFields, sortDirs);
	}

	/*
	 * Methods for parsing projection.
	 */

	private ProjectList projectList() {
		ProjectList list = new ProjectList();
		do {
			if (lex.matchDelim(','))
				lex.eatDelim(',');
			if (lex.matchId())
				list.addField(id());
			else {
				AggregationFn aggFn = aggregationFn();
				list.addAggFn(aggFn);
			}
		} while (lex.matchDelim(','));
		return list;
	}

	private AggregationFn aggregationFn() {
		AggregationFn aggFn = null;
		if (lex.matchKeyword("avg")) {
			lex.eatKeyword("avg");
			lex.eatDelim('(');
			aggFn = new AvgFn(id());
			lex.eatDelim(')');
		} else if (lex.matchKeyword("count")) {
			lex.eatKeyword("count");
			lex.eatDelim('(');
			if (lex.matchKeyword("distinct")) {
				lex.eatKeyword("distinct");
				aggFn = new DistinctCountFn(id());
			} else
				aggFn = new CountFn(id());
			lex.eatDelim(')');
		} else if (lex.matchKeyword("max")) {
			lex.eatKeyword("max");
			lex.eatDelim('(');
			aggFn = new MaxFn(id());
			lex.eatDelim(')');
		} else if (lex.matchKeyword("min")) {
			lex.eatKeyword("min");
			lex.eatDelim('(');
			aggFn = new MinFn(id());
			lex.eatDelim(')');
		} else if (lex.matchKeyword("sum")) {
			lex.eatKeyword("sum");
			lex.eatDelim('(');
			aggFn = new SumFn(id());
			lex.eatDelim(')');
		} else
			throw new UnsupportedOperationException();
		return aggFn;
	}

	/*
	 * Methods for parsing predicate.
	 */

	private Predicate predicate() {
		Predicate pred = new Predicate(term());
		while (lex.matchKeyword("and")) {
			lex.eatKeyword("and");
			pred.conjunctWith(term());
		}
		return pred;
	}

	private Term term() {
		Expression lhs = queryExpression();
		Term.Operator op;
		if (lex.matchDelim('=')) {
			lex.eatDelim('=');
			op = OP_EQ;
		} else if (lex.matchDelim('>')) {
			lex.eatDelim('>');
			if (lex.matchDelim('=')) {
				lex.eatDelim('=');
				op = OP_GTE;
			} else
				op = OP_GT;
		} else if (lex.matchDelim('<')) {
			lex.eatDelim('<');
			if (lex.matchDelim('=')) {
				lex.eatDelim('=');
				op = OP_LTE;
			} else
				op = OP_LT;
		} else
			throw new UnsupportedOperationException();
		Expression rhs = queryExpression();
		return new Term(lhs, op, rhs);
	}

	private Expression queryExpression() {
		return lex.matchId() ? new FieldNameExpression(id())
				: new ConstantExpression(constant());
	}

	/*
	 * Methods for parsing sort.
	 */

	private SortList sortList() {
		SortList list = new SortList();
		do {
			if (lex.matchDelim(','))
				lex.eatDelim(',');
			if (lex.matchId()) {
				String fld = id();
				int dir = sortDirection();
				list.addField(fld, dir);
			} else {
				AggregationFn aggFn = aggregationFn();
				int dir = sortDirection();
				list.addAggFn(aggFn, dir);
			}
		} while (lex.matchDelim(','));
		return list;
	}

	private int sortDirection() {
		int dir = DIR_ASC;
		if (lex.matchKeyword("asc")) {
			lex.eatKeyword("asc");
		} else if (lex.matchKeyword("desc")) {
			lex.eatKeyword("desc");
			dir = DIR_DESC;
		}
		return dir;
	}

	/*
	 * Methods for parsing the various update commands.
	 */

	public Object updateCommand() {
		if (lex.matchKeyword("insert"))
			return insert();
		else if (lex.matchKeyword("delete"))
			return delete();
		else if (lex.matchKeyword("update"))
			return modify();
		else if (lex.matchKeyword("create"))
			return create();
		else if (lex.matchKeyword("drop"))
			return drop();
		else
			throw new UnsupportedOperationException();
	}

	private InsertData insert() {
		lex.eatKeyword("insert");
		lex.eatKeyword("into");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		// Need to preserve the order of ids
		List<String> flds = idList();
		lex.eatDelim(')');
		lex.eatKeyword("values");
		lex.eatDelim('(');
		List<Constant> vals = constList();
		lex.eatDelim(')');
		return new InsertData(tblname, flds, vals);
	}

	private DeleteData delete() {
		lex.eatKeyword("delete");
		lex.eatKeyword("from");
		String tblname = lex.eatId();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new DeleteData(tblname, pred);
	}

	private ModifyData modify() {
		lex.eatKeyword("update");
		String tblname = lex.eatId();
		lex.eatKeyword("set");
		Map<String, Expression> map = new HashMap<String, Expression>();
		while (lex.matchId()) {
			String fldname = id();
			lex.eatDelim('=');
			Expression newval = modifyExpression();
			map.put(fldname, newval);
			if (lex.matchDelim(','))
				lex.eatDelim(',');
		}
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new ModifyData(tblname, map, pred);
	}

	private Expression modifyExpression() {
		if (lex.matchKeyword("add")) {
			lex.eatKeyword("add");
			lex.eatDelim('(');
			Expression lhs = queryExpression();
			lex.eatDelim(',');
			Expression rhs = queryExpression();
			lex.eatDelim(')');
			return new BinaryArithmeticExpression(lhs, OP_ADD, rhs);
		} else if (lex.matchKeyword("sub")) {
			lex.eatKeyword("sub");
			lex.eatDelim('(');
			Expression lhs = queryExpression();
			lex.eatDelim(',');
			Expression rhs = queryExpression();
			lex.eatDelim(')');
			return new BinaryArithmeticExpression(lhs, OP_SUB, rhs);
		} else if (lex.matchKeyword("mul")) {
			lex.eatKeyword("mul");
			lex.eatDelim('(');
			Expression lhs = queryExpression();
			lex.eatDelim(',');
			Expression rhs = queryExpression();
			lex.eatDelim(')');
			return new BinaryArithmeticExpression(lhs, OP_MUL, rhs);
		} else if (lex.matchKeyword("div")) {
			lex.eatKeyword("div");
			lex.eatDelim('(');
			Expression lhs = queryExpression();
			lex.eatDelim(',');
			Expression rhs = queryExpression();
			lex.eatDelim(')');
			return new BinaryArithmeticExpression(lhs, OP_DIV, rhs);
		} else if (lex.matchId())
			return new FieldNameExpression(id());
		else
			return new ConstantExpression(constant());
	}

	/*
	 * Method for parsing various create commands.
	 */

	private Object create() {
		lex.eatKeyword("create");
		if (lex.matchKeyword("table"))
			return createTable();
		else if (lex.matchKeyword("view"))
			return createView();
		else if (lex.matchKeyword("index"))
			return createIndex();
		else
			throw new UnsupportedOperationException();
	}

	private CreateTableData createTable() {
		lex.eatKeyword("table");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		Schema sch = fieldDefs();
		lex.eatDelim(')');
		return new CreateTableData(tblname, sch);
	}

	private Schema fieldDefs() {
		Schema schema = fieldDef();
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			Schema schema2 = fieldDefs();
			schema.addAll(schema2);
		}
		return schema;
	}

	private Schema fieldDef() {
		String fldname = id();
		return fieldType(fldname);
	}

	private Schema fieldType(String fldName) {
		Schema schema = new Schema();
		if (lex.matchKeyword("int")) {
			lex.eatKeyword("int");
			schema.addField(fldName, INTEGER);
		} else if (lex.matchKeyword("long")) {
			lex.eatKeyword("long");
			schema.addField(fldName, BIGINT);
		} else if (lex.matchKeyword("double")) {
			lex.eatKeyword("double");
			schema.addField(fldName, DOUBLE);
		} else {
			lex.eatKeyword("varchar");
			lex.eatDelim('(');
			double arg = lex.eatNumericConstant();
			lex.eatDelim(')');
			schema.addField(fldName, VARCHAR((int) arg));
		}
		return schema;
	}

	private CreateViewData createView() {
		lex.eatKeyword("view");
		String viewname = lex.eatId();
		lex.eatKeyword("as");
		QueryData qd = queryCommand();
		return new CreateViewData(viewname, qd);
	}

	private CreateIndexData createIndex() {
		lex.eatKeyword("index");
		String idxName = lex.eatId();
		lex.eatKeyword("on");
		String tblName = lex.eatId();
		lex.eatDelim('(');
		List<String> fldNames = idList();
		lex.eatDelim(')');
		
		// Index type
		IndexType idxType = DEFAULT_INDEX_TYPE;
		if (lex.matchKeyword("using")) {
			lex.eatKeyword("using");
			
			if (lex.matchKeyword("hash")) {
				lex.eatKeyword("hash");
				idxType = IndexType.HASH;
			} else if (lex.matchKeyword("btree")) {
				lex.eatKeyword("btree");
				idxType = IndexType.BTREE;
			} else
				throw new UnsupportedOperationException();
		}
		
		return new CreateIndexData(idxName, tblName, fldNames, idxType);
	}

	/*
	 * Method for parsing various drop commands.
	 */

	private Object drop() {
		lex.eatKeyword("drop");
		if (lex.matchKeyword("table"))
			return dropTable();
		else if (lex.matchKeyword("view"))
			return dropView();
		else if (lex.matchKeyword("index"))
			return dropIndex();
		else
			throw new UnsupportedOperationException();
	}

	private DropTableData dropTable() {
		lex.eatKeyword("table");
		String tblname = lex.eatId();
		return new DropTableData(tblname);
	}

	private DropViewData dropView() {
		lex.eatKeyword("view");
		String viewname = lex.eatId();
		return new DropViewData(viewname);
	}

	private DropIndexData dropIndex() {
		lex.eatKeyword("index");
		String idxname = lex.eatId();
		return new DropIndexData(idxname);
	}
}
