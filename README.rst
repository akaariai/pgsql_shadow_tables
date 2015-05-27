Audit table management for PostgreSQL 8.4+
------------------------------------------

An easy way to do database version tracking.

Three steps to version your data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Download shadow_upgrader.sql
#. Connect to wanted database, load the file with `\\i shadow_upgrader.sql`
#. Run `select shadow_meta.update_shadow_schema('public');` for each schema
   you want to version.

If you get errors in step 3 this is most likely because some of your tables do not
have primary keys. Add such tables to shadow_meta.skip_tables by running::

    insert into shadow_meta.skip_tables(tablename, schemaname)
    values ('the_table', 'the_schema');

At this point any INSERT, UPDATE or DELETE will be recorded to table shadow_<original_schema_name>.__shadow_<original_table_name>.
So, for example changes to public.book will be tracked in shadow_public.__shadow_book.

Two more steps to query historical data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Inside a transaction, run SELECT shadow_meta.timetravel('public', '2015-01-01'::timestamptz);.
#. Run normal SQL agaist historical view of the database.

Of course, in step 1 replace the schema name and the timestamp with some point in
time which you want to query.

At this point you have complete row based versioning for your database, and you
have the ability to "timetravel" in your database. Note that you need zero changes
to your application for versioning, and you can use your original queries against
historical data.

If you do schema changes to your tables just run again `SELECT shadow_meta.update_shadow_schema('public');`.
