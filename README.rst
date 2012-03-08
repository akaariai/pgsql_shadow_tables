Experimental shadow-table management for PostgreSQL.
----------------------------------------------------

First warnings: this project is very experimental at the moment. I haven't
used this in production. I have not tested this enough. So, use at your own
risk. It would not surprise me if this eats your production data.

Might work on 8.4+. I strongly recommend not even trying this on a production
database. Use at your own risk.

Usage::

  psql -d wanted_database
  create language plpgsql; -- if not already installed.
  \i shadow_upgrader.sql
  -- repeat the following for every schema you want to track.
  select shadow_meta.update_shadow_schema('public');

That's it. If the above completed without errors, you should now have
audit-triggers for every table in the public schema. The most common error
you will get is one about not finding primary keys. That one can be solved
in three ways.

  1. Add the table into shadow_meta.skip_tables::

       insert into shadow_meta.skip_tables(tablename, schemaname)
         values('public', 'problematic_table');
     Doing this will result in that table missing audit-logging.
  2. Add a primary key for the table.
  3. Drop the table.

It is worth noting that generally you do _not_ want audit-log all the tables.
For example, web-session tables are not worth audit-logging. Similarly, insert
only tables should be skipped. In addition you might want to skip large tables,
the audit-logging implementation will use as much space as the original table,
even without any updates. If you have a lot of small updates to a large table,
this audit-logging implementation is not likely for you.

There is another configuration table worth noting: shadow_meta.shadow_config.
It currently contains just one configuration parameter: the session variable
name. We will get back to that soon.

It is worth investigating what the shadow table contains, lets assume you have
a table named sometable(id integer primary key, col text not null). That one
will get a matching shadow table by name __shadow_sometable. The prefix is
currently hard-coded, as is the `shadow_` prefix for the shadow schema name.

The shadow table will have the same columns as the original table with no
constraints. The reason for no constraints is that this makes updating the
shadow table schema much easier. In addition it has four columns:
  - __insert_ts, __insert_tx: this pair identifies the inserting transaction
  - __del_ts, __del_tx: this pair identifies the removing transaction
Now, look at the table contents::

  select * from shadow_public.__shadow_sometable;

It should contain a row for every existing row in the original table. Of course
if the original table was empty, so is the shadow table. I suggest you do a
insert, update and then a delete into the original table. This should be run
with auto-commit on (in other words, each command in separate transaction)::

  insert into sometable values(1, 'foo');
  update sometable set col = 'bar' where id = 1;
  delete from sometable where id = 1;

Now you should have two rows in the __shadow_sometable. Lets see how those
got constructed. I am going to use insert_id and del_id below as shorthands for
the pairs __insert_ts, __insert_tx and __del_ts, __del_tx. So, the first insert
created a row::

  1, 'foo', insert_id1, null

Then the update changes that row to::

  1, 'foo', insert_id1, del_id2

And adds another row::

  1, 'bar', insert_id2, null

The del finally changes the last row to::

  1, 'bar', insert_id2, del_id3

If you look at the time ranges of the __insert_ts, __del_ts columns you will
note that they form a continious range, and you can check what value was in
effect at any given moment using only those two columns.

So, why the _tx columns? The idea is that using those it is possible to
identify the transaction doing the modification, and in addition these values
are easily available without any session variables or other modifications. Now,
if you want to know who did the modifications, you would need to add a mod_log
table::

  create table mod_log(
    username text,
    when_ts timestamptz not null default now(),
    when_tx bigint not null default txid_current(),
    primary key (when_ts, when_tx)
  );

Now, in any transaction doing modifications to the audit-logged tables, first
do an insert to the mod_log::

  insert into mod_log(username) values('akaariai');

Now you also have knowledge of _who_ did the modifications. There isn't
support for foreign keys from the shadow tables to the mod_log table. That
will likely be added in the future, as that allows the database to enforce
that there is always knowledge of the user doing the modifications. (This can
also be somewhat annoying, as you can't modify the logged tables outside an
explicit transaction).

NOTE: You really, really want to use the database now() in the mod_log table,
not now() from Python/Java code. The logging must be ran IN THE SAME
TRANSACTION as the modifications. Otherwise the linking between the user and 
he modifications will be lost.

Now for a little trick. This requires you to have test_session_variable in your
postgresql.conf custom_variable_classes. So, in practice you need
admin-privileges to the DB server to test this out. Do this::

   set search_path = 'shadow_public, public';
   set test_session_variable.view_time = 'wanted view timestamp';
   -- for example '2012-05-06 22:08:00'

And now you can "timetravel" your database as you wish using your existing
queries (assuming you are not using schema qualified names in your existing
queries). The shadow view works on this trick::

    create view shadow_schema.sometable as
       select * from shadow_schema.__shadow_sometable
         where __insert_ts <= current_setting('test_session_variable.view_time')::timestamptz
               and (__del_ts is null or
                    __del_ts > current_setting('test_session_variable.view_time')::timestamptz);

The whole idea is that the view looks like the real table for select queries,
it shows a "snapshot" of the table at the selected view time. As you have the
shadow schema name before the real schema name in the search_path, the view is
spotted before the real table by PostgreSQL.

Using the above trick you get a snapshot of the _whole_ database. The last
part can be a problem, too. If you need finer granularity, you will need to
write the queries by hand.

After you have altered some tables, or added new tables::

   select shadow_meta.update_shadow_schema('public');

The shadow schema should be upgraded, as well as the views and triggers.

Known limitations:
  - As said above sometimes eats your data.
  - The tracking is based on primary key. This has two consequences:
    1. Tables not having primary keys can not be tracked.
    2. Updatable primary keys work, but the chain of history is broken in the
       shadow table. That is, you have:
         oldpk, yesterday, today
         newpk, today, -
       when you try to check the history and you only know newpk, you are kind
       of lost.
       
       In short: if you need to track some object, you want to either know its
       primary key history, or better yet, have immutable primary key.
  - Eats a lot of space: the shadow table will be _at minimum_ 2x the size of
    the original table. If you do a lot of updates, it will soon be really
    large. This is because tracking is based on saving the full row versions
    for each modification, not just the modified data.
  - Query plans from the shadow views can be pretty bad. The shadow tables do
    not have indexes.
  - You can't say what was visible at given moment or to given transaction in
    the database. A concurrent transaction might have been visible or not,
    depending on interleaving of the transactions. As said, that information
    isn't available. This is mostly a non-issue, but if you need this
    information, you won't get it 100% guaranteed by using this project.
  - Concurrent edits to the same row might cause errors which would not happen
    without shadow tables.
    
I have used a similar system for some production systems. In my opinion this
works really nicely for small databases which do not have a lot of
modifications. If you have a large database, or your database is write-heavy,
you probably do not want to use this kind of modification logging, at least
not for all tables.

If you have ideas how to improve the implementation, or feature request, please
drop me a message or create a issue.

Last: this _really_ isn't tested. Use at your own risk!
