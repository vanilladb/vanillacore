# VanillaDB SQL

VanillaDB is a teaching-purpose prototype. It deliberately implements a tiny subset of SQL, and (for simplicity) imposes restrictions not present in the SQL standard. Here we briefly indicate these restrictions.

## Restrictions

A query in VanillaDB consists only of select-from-where clauses in which the select clause contains a list of fieldnames (without the AS keyword), and the from clause contains a list of tablenames (without range variables).

The where clause is optional. The only Boolean operator is and. Unlike standard SQL, there are no other Boolean operators and no parentheses.
The group by, order by clauses and partial aggregation functions are supported. Arithmetic expression is only supported in update command.

Views can be created, but a view definition can be at most 100 characters.
Because there are no renaming, all field names in a query must be disjoint.

Other restrictions:
- The `*` abbreviation in the select clause is not supported.
- There are no null values.
- There are no explicit joins or outer joins in the from clause.
- The union and except keywords are not supported.
- Insert statements take explicit values only, not queries.
- Update statements can have only one assignment in the set clause.

## Syntax

### Predicates

```
<Field>         := IdTok
<Constant>      := StrTok | NumericTok
<Expression>    := <Field> | <Constant>
<BinaryArithmeticExpression>    :=
                                ADD(<Expression>, <Expression>) |
                                SUB(<Expression>, <Expression>) |
                                MUL(<Expression>, <Expression>) |
                                DIV(<Expression>, <Expression>)
<Term>  :=
        <Expression> = <Expression>  |
        <Expression> > <Expression>  |
        <Expression> >= <Expression> |
        <Expression> < <Expression>  |
        <Expression> <= <Expression>
<Predicate> := <Term> [ AND <Predicate> ]
```

### Queries

```
<Query>     := SELECT <ProjectSet> FROM <TableSet>
            [ WHERE <Predicate> ] [ GROUP BY <IdSet> ]
            [ ORDER BY <SortList> [ DESC | ASC ] ]
<IdSet>     := <Field> [ , <IdSet> ]
<TableSet>  := IdTok [ , <TableSet> ]
<AggFn>     := AVG(<Field>) | COUNT(<Field>) |
            COUNT(DISTINCT <Field>) | MAX(<Field>) |
            MIN(<Field>) | SUM(Field>)
<ProjectSet>    := <Field> | <AggFn> [ , <ProjectSet>]
<SortList>      := <Field> | <AggFn> [ , <SortList>]

```

### Updates

```
<UpdateCmd>         := <Insert> | <Delete> | <Modify> | <Create> | <Drop>
<Create>            := <CreateTable> | <CreateView> | <CreateIndex>
<Drop>              := <DropTable> | <DropView> | <DropIndex>
<Insert>            := INSERT INTO IdTok ( <FieldList> )
                       VALUES ( <ConstantList> )
<FieldList>         := <Field> [ , <Field> ]
<ConstantList>         := <Constant> [ , <Constant> ]
<Delete>            := DELETE FROM IdTok [ WHERE <Predicate> ]
<Modify>            := UPDATE IdTok SET <ModifyTermList>
                        [ WHERE <Predicate> ]
<ModifyExpression>  := <Expression> | <BinaryArithmeticExpression>
<ModifyTermList>    := <Field> = <ModifyExpression> [ , <ModifyTermList> ]
<CreateTable>       := CREATE TABLE IdTok ( <FieldDefs> )
<DropTable>         := DROP TABLE IdTok
<FieldDefs>         := <FieldDef> [ , <FieldDef> ]
<FieldDef>          := IdTock <TypeDef>
<TypeDef>           := INT | LONG | DOUBLE | VARCHAR ( NumericTok )
<CreateView>        := CREATE VIEW IdTok AS <Query>
<DropView>          := DROP VIEW IdTok
<CreateIndex>       := CREATE INDEX IdTok ON IdTok ( <IdSet> ) [ USING HASH | BTREE ]
<DropIndex>         := DROP INDEX IdTok
```
