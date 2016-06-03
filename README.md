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

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
