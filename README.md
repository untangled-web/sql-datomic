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
- Raw attribute keywords may be used in place of column names:
    (e.g., using `#attr :product/title` directly instead of
    `product.title`).
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
- `WHERE` clauses support `AND`, `OR`, and `NOT` terms.
- `IN` clauses are supported in `WHERE`s.
- `BETWEEN` clauses must operate on a column, and use either
  numeric literals or chronological literals (`DATE`, `DATETIME`, `TIME`)
  for bounds.
- Supported scalar types are:
    - boolean: `TRUE`, `FALSE` (`true`, `false`)
    - numeric (int, float): `42`, `3.14159`, `6.62607004e-34`
        - int-types:
            - long: `42`
            - bigint: `9001N`
        - float-types:
            - double: `3.14159`
            - float:  `2.7182F`, `1.6182f`, `#float -5.24e-5`
            - bigdec: `24.95M` (often used for monetary values)
    - strings: `''`, `'foo'`, `'bIlujDI\\' yIchegh()Qo\\'; yIHegh()!'`
    - chronological literals (all assumed to be UTC):
        - `date '1999-12-31'`
        - `datetime '1970-04-01T09:01:00'`
        - `time '1465423112'`
- Supported comparison operators:
    - `=`, `<>`, `!=` for columns and all scalars
    - `<`, `<=`, `>`, `>=` for columns, numeric and chronological types

### Datomic-native data types supported in SQL dialect

In addition to the usual data types expected in a database
(e.g., string, ints, floats, booleans), Datomic also supports some
interesting additional data types.  To better support them at the
SQL prompt, the following Tagged Literals are supported:

- UUID: `#uuid "5760745a-5bb5-4768-96f7-0f8aeb1a84f0"`
- URI: `#uri "http://slashdot.org/"`
- Byte Array (base-64): `#bytes "b2hhaQ=="`
- Instant (like datetimes): `#inst "1987-01-14T10:30:00"`
- Keyword: `:foo.bar/baz-quux`, `:ohai`
- Float: `#float 3.1415`

Note that byte arrays are not value types (from Datomic's perspective);
therefore, they are not supported in `WHERE` clauses.

For more about Datomic native types, please
[read the fine documentation](http://docs.datomic.com/schema.html#text-1-1).

## Shortened convenience forms

- select:
    - `select where #attr :product/prod-id between 1567 and 6000`
- insert:
    - `insert into
         #attr :product/prod-id = 1984,
         #attr :product/actor = 'Quux',
         #attr :product/title = 'Foo Bar',
         #attr :product/price = 21.99M`
- delete:
    - `delete where #attr :product/prod-id between 1567 and 6000`
- update:
    - `update product.rating = 3.5f where product.prod-id = 1567`

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
