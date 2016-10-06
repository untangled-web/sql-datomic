# sql-datomic

Interpreter of a SQL-ish dialect that runs against Datomic databases.

Designed to translate basic SQL statements into
legal Datomic-compatible datalog.

Assumes that the Datomic schema makes use of the `:entity/attribute`
convention for all attributes.

## Usage

To use the interpreter (REPL), run:
```
  $ bin/repl
  ## connects to a default mem db.
```
or even:
```
  $ rlwrap lein run
```
To run it against a non-mem Datomic db, run:
```
  $ bin/repl -u $datomic_connect_uri
  ## same kind of URI you would hand to datomic.api/connect.
```

To run the tests, run:
```
  $ lein test
```

For a list of command-line flags available to the interpreter, run:
```
  $ bin/repl --help
```
or equivalently:
```
  $ lein run -- --help
```

For a list of supported in-interpreter commands, run:
```
  sql> help
```
from within a running interpreter.

## Sample session

```
  sql> \d
  sql> \dn
  sql> \x
  sql> select where product.prod-id = 9990
  sql> \x
  sql> debug
  sql> select where order.orderdate between #inst "2004-01-01" and #inst "2004-01-05"
  sql> select product.prod-id, #attr :product/tag, product.title where product.prod-id = 9990
  sql> select db.id, customer.city, customer.state, customer.zip from customer where customer.customerid = 4858
  sql> debug
  sql> update customer set customer.city = 'Springfield', customer.state = 'VA', customer.zip = '22150' where customer.customerid = 4858
  sql> select db.id, customer.city, customer.state, customer.zip from customer where customer.customerid = 4858
  sql> \x
  sql> select where product.prod-id = 9999
  sql> insert into #attr :product/prod-id = 9999, #attr :product/actor = 'Naomi Watts', #attr :product/title = 'The Ring', product.category = :product.category/horror, product.rating = 4.5f, product.man-hours = 9001N, product.price = 21.99M
  sql> select where product.prod-id = 9999
  sql> delete from product where product.prod-id = 9999
  sql> select where product.prod-id = 9999
  sql> \d order
  sql> \d orderline
  sql> select where order.orderid > 0
  sql> select where orderline.orderlineid > 0
  sql> delete from order where order.orderid > 0
  sql> \d order
  sql> \d orderline
  sql> select where order.orderid > 0
  sql> select where orderline.orderlineid > 0
  sql> \x
  sql> \d customer
  sql> select db.id, #attr :customer/customerid, #attr :customer/username, #attr :customer/password from customer where customer.customerid > 0
  sql> pretend
  sql> update customer set customer.username = 'donald.duck', customer.password = 'somethingclever' where customer.customerid = 14771
  sql> pretend
  sql> debug
  sql> select db.id, #attr :customer/customerid, #attr :customer/username, #attr :customer/password from customer where customer.customerid > 0
  sql> update customer set customer.username = 'donald.duck', customer.password = 'somethingclever' where customer.customerid = 14771
  sql> select db.id, #attr :customer/customerid, #attr :customer/username, #attr :customer/password from customer where customer.customerid > 0
```

## Differences from standard SQL

The SQL used by this tool is largely a subset of ANSI SQL-92, with the
following deviations:

- Accepts only `SELECT`, `INSERT`, `UPDATE`, `DELETE` statements.
- Adds the notion of a `RETRACT` statement as a way to drop a field
  from a given "row" (really, entity).  Affects that entity alone; does
  not affect schema for other rows / entities:
    `RETRACT actor.realname WHERE db.id = 123245`
  (In Datomic parlance, this retracts that E-A-V fact; V is gathered
   automatically on the user's behalf.  For more on retraction,
   [please refer to the documentation](http://docs.datomic.com/transactions.html#retracting-data).)
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
- ~~Aliases are permitted on table names:~~
    - ~~`select "la la la".foo from bar "la la la"`~~
    - ~~`select lalala.foo from bar as lalala`~~
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
SQL prompt, the following are supported (most with Tagged Literals):

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

For more about Tagged Literals, please
[have a look at this](http://clojure.org/reference/reader#_tagged_literals).

## Shortened convenience forms

- select:
    - `select where #attr :product/prod-id between 1567 and 6000`
- insert:
    - `insert
         #attr :product/prod-id = 1984,
         #attr :product/actor = 'Quux',
         #attr :product/title = 'Foo Bar',
         #attr :product/price = 21.99M`
- delete:
    - `delete where #attr :product/prod-id between 1567 and 6000`
- update:
    - `update product.rating = 3.5f where product.prod-id = 1567`

## Currently supported command-line flags:

```
 Switches                       Default     Desc
 --------                       -------     ----
 -h, --no-help, --help          false       Print this help
 -d, --no-debug, --debug        false       Write debug info to stderr
 -p, --no-pretend, --pretend    false       Run without transacting; turns on debug
 -x, --no-expanded, --expanded  false       Display resultsets in expanded output format
 -u, --connection-uri                       URI to Datomic DB; if missing, uses default mem db
 -s, --default-schema-name      :dellstore  :dellstore or :starfighter, for default in-mem db
```

## Currently supported interpreter commands:

```
type `exit` or `quit` or ^D to exit
type `debug` to toggle debug mode
type `pretend` to toggle pretend mode
type `expanded` or `\x` to toggle expanded display mode
type `show tables` or `\d` to show Datomic "tables"
type `show schema` or `\dn` to show all user Datomic schema
type `describe $table` or `\d $table` to describe a Datomic "table"
type `describe $dbid` or `\d $dbid` to describe the schema of an entity
type `status` to show toggle values, conn strings, etc.
type `\?`, `?`, `h` or `help` to see this listing
```

## Running the test suite

```
  $ lein test
```

## Caveats

This is a rather sharp tool (although the `pretend` flag helps).  It will carry out
all mutating statements immediately (i.e., behaves as if run in an AUTOCOMMIT mode).
Again, judicious use of `pretend` can help immensely.

## TODO

- Finish support for aliases on table names; they parse correctly, but
  the query engine does not yet understand them.
- Support some kind of multiline statement input from within the
  interpreter; the parser and the query engine have no trouble with
  multiline input (see the tests).

## License

The MIT License (MIT) Copyright © 2016 NAVIS
