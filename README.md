# sql-datomic

A Clojure library designed to translate basic SQL statements into
legal Datomic-compatible datalog.

Assumes that the Datomic schema makes use of the `:entity/attribute`
convention for all attributes.

## Usage

To use the REPL, run:
```
  $ bin/repl
```
or even:
```
  $ rlwrap lein run
```

To run the tests, run:
```
  $ lein test
```

## Differences from standard SQL

The SQL used by this tool is largely a subset of ANSI SQL-92, with the
following deviations:

- Accepts only `SELECT`, `INSERT`, `UPDATE`, `DELETE` statements.
- Column names must be fully qualified (`table_name.column_name`)
  for `select`, `update` and `delete`; `insert` are exempt from this.
- No support for explicit `JOIN`s.  Instead, use implicit joins
  (which is more like Datomic's syntax anyhow).
- No support for `NULL`, `IS NULL`, `IS NOT NULL` (no meaning in Datomic).
- No support for general arithmetic operators, string operators
  or any functions.
- Hyphens are permitted in table names and column names:
  `survey-request.sent-date`.  (Helps with Datomic interop.)
- Aliases are permitted on table names:
    - `select "la la la".foo from bar "la la la"`
    - `select lalala.foo from bar as lalala`
- `FROM` clauses must consist of table names only (no subselects).
- `WHERE` clauses support `AND` terms only.
- `BETWEEN` clauses must operate on a column, and use either
  numeric literals or chronological literals (`DATE`, `DATETIME`, `TIME`)
  for bounds.
- Supported scalar types are:
    - boolean: `TRUE`, `FALSE`
    - numeric (int, float): `42`, `3.14159`, `6.62607004e-34`
    - strings: `''`, `'foo'`, `'bIlujDI\\' yIchegh()Qo\\'; yIHegh()!'`
    - chronological literals (all assumed to be UTC):
        - `date '1999-12-31'`
        - `datetime '1970-04-01T09:01:00'`
        - `time '1465423112'`
- Supported comparison operators:
    - `=`, `<>`, `!=` for columns and all scalars
    - `<`, `<=`, `>`, `>=` for columns, numeric and chronological types

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
