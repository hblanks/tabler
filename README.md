# tabler

tabler writes input data into tables in a SQLite or PostgreSQL DB. It's the
"load" in Extract-Transform-Load, and a fairly minimal one at that.

Caveats:

* *tabler is by no means ready for production use!* It lacks tests, Travis
  integration, and has not yet been released in any way.

Features:

* reads from stdin or TCP socket
* parses JSON or Heka protobuf messages (including Heka streaming format)


## Building tabler

tabler is built using [gb](https://getgb.io/). If you don't have `gb` installed,
first do:

    go get github.com/constabulary/gb/...

Then, cd into a checkout of the `tabler` repo and do:

    gb build

That's it.

## Using tabler

tabler works in two steps:

1. Create a `tables.json` describing your tables. The easiest way to do this is to
   feed a representative sample of your data into `tabler -generate-tables`.

2. Send data into `tabler TABLES_JSON DSN`, where `TABLES_JSON` is the path to
   your `tables.json` file, and DSN is a SQLite or PostgreSQL DSN.

tabler does have unfair assumptions about your data. Specifically, if it's JSON,
it expects each object to have a "type" property, which specifies which table
that data should be inserted into. Making it so that any property name could be
used would be a small patch, though.
