begin;

/* shadow_meta is a schema where the configuration tables
 * and plpgsql functions are stored
 */
drop schema if exists shadow_meta cascade;
create schema shadow_meta;

create table shadow_meta.shadow_config(
    varname varchar(20) not null check (
        varname in ('session_variable')),
    val varchar(100) not null,
    primary key (varname, val)
);
/* I think later on this might be better to be shadow_meta.table_config
 * so that you could have more options than just "skip"
 */
create table shadow_meta.skip_tables(
    tablename varchar(64) not null,
    schemaname varchar(64) not null,
    primary key(tablename, schemaname)
);

create function shadow_meta.current_view_time() returns timestamptz as $$
    declare
        ret timestamptz;
    begin
	select dest from timetravel into ret;
	return ret;
    end
$$
language plpgsql stable;

/* These functions would be much nicer to write in PL/Python, but I
 * don't want to add dependencies...
 */
create function shadow_meta.create_shadow_table(_schema text, tablename text) returns void as
$$
    declare
        colinfo record;
        col_definition text = ''; 
        pk_column text;
        table_oid record; -- What is the correct type? Use record.oid for now...
        shadow_schema_name text;
    begin
        shadow_schema_name = 'shadow_'||_schema;
        -- We start by fetching the columns and datatypes of the table.
        -- We do not care about the constraints and indexes, as they do not belong
        -- into the shadow table.
        --
        -- The table definition is:
        --     real columns,
        --     __insert_ts timestamp with time zone not null default now(),
        --     __insert_tx bigint not null default txid_current(),
        --     __del_ts timestamp with time zone,
        --     __del_tx bigint,
        --     primary key (real_pk + __insert_ts)
        -- Multiple edits done in a single transaction are collapsed into one.
        -- 
        -- We follow what psql does (psql -E, \d tablename).
        -- Start by fetching the table OID.
    SELECT c.oid
          FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = tablename
           AND n.nspname = _schema into table_oid;
        
       if table_oid is null then
            raise exception 'Got null OID for table, does %.% really exist?', _schema, tablename;
        end if;
        
        -- Fetch the column names and datatypes from pg_catalog. Using information_schema
        -- would be nicer, but it seems hard to get a datatype usable directly in query strings
        -- from there.
        for colinfo in (SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) datatype
                          FROM pg_catalog.pg_attribute a
                         WHERE a.attrelid = table_oid.oid 
                               AND a.attnum > 0 AND NOT a.attisdropped
                         ORDER BY a.attnum) loop
            if colinfo is null or colinfo.attname is null or colinfo.datatype is null THEN
                raise exception 'Problem figuring out column name or datatype for table %.%.', _schema, tablename;
            end if;
            col_definition = col_definition||' '||quote_ident(colinfo.attname)||' '||colinfo.datatype||', ';
        end loop;
        -- Add the shadow columns.
        col_definition = col_definition||'__insert_ts timestamp with time zone not null default now(), __insert_tx bigint not null default txid_current(), ';
        col_definition = col_definition||'__del_ts timestamp with time zone check (__del_ts > __insert_ts), __del_tx bigint, ';
        select array_to_string(array_agg(quote_ident(column_name)), ', ')
          from (
              select column_name
                from information_schema.table_constraints tc
                join information_schema.key_column_usage kcu
                     on kcu.table_schema = tc.table_schema
                     and kcu.constraint_name = tc.constraint_name
               where tc.table_name = tablename
                     and tc.table_schema = _schema
                     and tc.constraint_type = 'PRIMARY KEY'
               order by ordinal_position) tmp
          into pk_column;
        if pk_column is null then
            raise exception 'Problem figuring out primary key for table %.%', _schema, tablename;
        end if;
        col_definition = col_definition||'PRIMARY KEY('||pk_column||', __insert_ts)';
        execute 'CREATE TABLE '||quote_ident(shadow_schema_name)||'.'||quote_ident('__shadow_'||tablename)||'('||
                          col_definition||');';
    end
$$
language plpgsql security definer volatile set client_min_messages = warning;

create function shadow_meta.create_triggers(_schema text, tablename text) returns void as
$$
    declare
        insert_cols text = '';
        valuesclause text = '';
        updatecols text = '';
        table_oid record;
        shadow_schema_name text;
        pk_col_clause text;
    begin
        /* Done similarly to create_shadow_table. */
        shadow_schema_name = 'shadow_'||_schema;
        -- Fetch the table OID.
        SELECT c.oid
          FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = tablename
           AND n.nspname = _schema into table_oid;
        if table_oid is null then
            raise exception 'Got null OID for table, does %.% really exist?', _schema, tablename;
        end if;
        
        -- Fetch the column names from pg_catalog (done for consistency to
        -- create_shadow_table).
        SELECT array_to_string(array_agg(quote_ident(a.attname)), ', '),
               array_to_string(array_agg('new.'||quote_ident(a.attname)), ', '),
               array_to_string(array_agg(quote_ident(a.attname)|| ' = new.'||quote_ident(a.attname)), ', ')
          FROM (select a.attname
                  from pg_catalog.pg_attribute a
                 WHERE a.attrelid = table_oid.oid 
                   AND a.attnum > 0 AND NOT a.attisdropped
                 ORDER BY a.attnum) a 
                  INTO insert_cols, valuesclause, updatecols;
         -- Insert trigger function.
         execute 'CREATE OR REPLACE FUNCTION '||quote_ident(shadow_schema_name)||'.'||quote_ident('__trg_on_'||tablename||'_add')||E'() returns trigger as $_$\n'||
                 E' DECLARE\n'||
                 E' BEGIN\n'||
                 '  INSERT INTO '||quote_ident(shadow_schema_name)||'.'||quote_ident('__shadow_'||tablename)||'('||insert_cols||') VALUES ('||valuesclause||E');\n'||
                 E' RETURN NEW;\n'||
                 E' END\n'||
                 E'$_$\n'||
                 'LANGUAGE PLPGSQL SECURITY DEFINER VOLATILE;';
         execute 'drop trigger if exists '||quote_ident(tablename||'_on_add_trg')||' ON '||quote_ident(_schema)||'.'||quote_ident(tablename);
         execute 'create trigger '||quote_ident(tablename||'_on_add_trg')||' AFTER INSERT ON '||quote_ident(_schema)||'.'||quote_ident(tablename)||
                 ' for each row execute procedure '||quote_ident(shadow_schema_name)||'.'||quote_ident('__trg_on_')||tablename||'_add'||'();';
         -- UPDATE & DELETE triggers
         select array_to_string(array_agg(quote_ident(column_name)||' = old.'||quote_ident(column_name)), ' AND ')
          from (
              select column_name
                from information_schema.table_constraints tc
                join information_schema.key_column_usage kcu
                     on kcu.table_schema = tc.table_schema
                     and kcu.constraint_name = tc.constraint_name
               where tc.table_name = tablename
                     and tc.table_schema = _schema
                     and tc.constraint_type = 'PRIMARY KEY'
               order by ordinal_position) tmp
          into pk_col_clause;
         execute 'CREATE OR REPLACE FUNCTION '||quote_ident(shadow_schema_name)||'.'||quote_ident('__trg_on_'||tablename||'_mod')||E'() returns trigger as $_$\n'||
                 E' DECLARE\n'||
                 E'  last_ts timestamptz;\n'||
                 E' BEGIN\n'||
                 E'  IF TG_OP = \'UPDATE\' AND new IS NOT DISTINCT FROM old THEN\n'||
                 E'      RETURN NEW;\n'||
                 E'  END IF;\n'||
                 E'  SELECT __insert_ts FROM '||quote_ident(shadow_schema_name)||'.'||quote_ident('__shadow_'||tablename)||
                 E'   WHERE __del_ts IS NULL AND '||pk_col_clause||E' into last_ts;\n'||
                 E'  IF last_ts <> now() THEN\n'||
                 E'   UPDATE '||quote_ident(shadow_schema_name)||'.'||quote_ident('__shadow_'||tablename)||' SET __del_ts = now(), __del_tx = txid_current() '||
                 E'    WHERE __del_ts IS NULL AND '||pk_col_clause||E';\n'||
                 E'   IF TG_OP = \'UPDATE\' THEN\n'||
                 '      INSERT INTO '||quote_ident(shadow_schema_name)||'.'||quote_ident('__shadow_'||tablename)||'('||insert_cols||') VALUES ('||valuesclause||E');\n'||
                 E'   END IF;\n'||
                 E'  ELSE\n'||
                 -- We are modifying the same row again inside the same transaction. If OP == del, remove the new shadow row,
                 -- else update it.
                 E'   IF TG_OP = \'UPDATE\' THEN\n'||
                 E'    UPDATE '||quote_ident(shadow_schema_name)||'.'||quote_ident('__shadow_'||tablename)||' SET '||updatecols||
                 E'     WHERE __insert_ts = last_ts AND '||pk_col_clause||E';\n'||
                 E'   ELSE\n'||
                 E'     DELETE FROM '||quote_ident(shadow_schema_name)||'.'||quote_ident('__shadow_'||tablename)||
                 E'     WHERE __insert_ts = last_ts AND '||pk_col_clause||E';\n'||
                 E'   END IF;\n'||
                 E'  END IF;\n'||
                 E' RETURN NEW;\n'||
                 E' END\n'||
                 E'$_$\n'||
                 'LANGUAGE PLPGSQL SECURITY DEFINER VOLATILE;';
         execute 'drop trigger if exists '||quote_ident(tablename||'_on_mod_trg')||' ON '||quote_ident(_schema)||'.'||quote_ident(tablename);
         execute 'create trigger '||quote_ident(tablename||'_on_mod_trg')||' AFTER UPDATE OR DELETE ON '||quote_ident(_schema)||'.'||quote_ident(tablename)||
                 ' for each row execute procedure '||quote_ident(shadow_schema_name)||'.'||quote_ident('__trg_on_')||tablename||'_mod'||'();';        
    end 
$$
language plpgsql security definer volatile set client_min_messages = warning;

create or replace function shadow_meta.create_view(_schema text, tablename text) returns void as $$
  declare
      shadow_schema_name text;
  begin
      shadow_schema_name = 'shadow_'||_schema;
      EXECUTE 'drop view if exists '||quote_ident(shadow_schema_name)||'.'||quote_ident(tablename);
      EXECUTE 'create view '||quote_ident(shadow_schema_name)||'.'||quote_ident(tablename)||' AS'||
              ' SELECT * FROM (select * from '||quote_ident(shadow_schema_name)||'.'||quote_ident('__shadow_'||tablename)||') tmp' ||
              '  WHERE __insert_ts <= (select shadow_meta.current_view_time()) ' ||
              '    AND (__del_ts IS NULL OR __del_ts > (select shadow_meta.current_view_time()))';
  end 
$$
language plpgsql security definer volatile set client_min_messages = warning;

create or replace function shadow_meta.ensure_shadow_schema(_schema text, recreate boolean default false) returns void as $$
  declare
      shadow_schema_name text;
      already_exists boolean;
  begin
      shadow_schema_name = 'shadow_'||_schema;
      if exists (select true from information_schema.schemata where schema_name = shadow_schema_name) then
          already_exists = true;
      else
          already_exists = false;
      end if;
      if already_exists and recreate then
          -- This is a pretty dangerous operation. There went your audit history!
          execute 'drop schema '||quote_ident(shadow_schema_name)||' cascade'; 
      end if;
      if not already_exists or recreate then
          execute 'create schema '||quote_ident(shadow_schema_name);
      end if;
  end 
$$
language plpgsql security definer volatile set client_min_messages = warning;
create or replace function shadow_meta.modify_table(_schema text, tablename text) returns boolean as $$
    declare
        colinfo record;
	shadow_colinfo record;
        col_definition text = ''; 
        pk_column text;
        table_oid record; -- What is the correct type? Use record.oid for now...
        shadow_table_oid record; -- What is the correct type? Use record.oid for now...
        shadow_schema_name text;
        modifications boolean = false;
    begin
        /* Once again we use best software development practices: copy-paste all the stuff. */
        shadow_schema_name = 'shadow_'||_schema;
        -- Fetch the table OID.
        SELECT c.oid
          FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = tablename
           AND n.nspname = _schema into table_oid;
        -- Fetch the table OID.
        SELECT c.oid
          FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = quote_ident('__shadow_'||tablename) -- Wonder if this really works on special ident chars...
           AND n.nspname = quote_ident('shadow_'||_schema) into shadow_table_oid;
        
       if table_oid is null then
            raise exception 'Got null OID for table, does %.% really exist?', _schema, tablename;
        end if;
        
        for colinfo in ((SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) datatype
                          FROM pg_catalog.pg_attribute a
                         WHERE a.attrelid = table_oid.oid 
                               AND a.attnum > 0 AND NOT a.attisdropped
                         ORDER BY a.attnum)
                         /* Fetch all columns, except the ones that already exists */
                        EXCEPT
                        (SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) datatype
                          FROM pg_catalog.pg_attribute a
                         WHERE a.attrelid = shadow_table_oid.oid 
                               AND a.attnum > 0 AND NOT a.attisdropped
                         ORDER BY a.attnum)) loop
            if colinfo is null or colinfo.attname is null or colinfo.datatype is null THEN
                raise exception 'Problem figuring out column name or datatype for table %.%.', _schema, tablename;
            end if;
	    -- Check if the type of the column has changed.
            SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) datatype
	      FROM pg_catalog.pg_attribute a
	     WHERE a.attrelid = shadow_table_oid.oid
	           AND a.attnum > 0 AND NOT a.attisdropped
	           AND a.attname = colinfo.attname
	     INTO shadow_colinfo;
            modifications = true;
	    IF shadow_colinfo is not null and shadow_colinfo.datatype != colinfo.datatype THEN
		-- This works if the datatype can be auto-coerced. If not, the user must do the datatype
                -- change manually.
		execute 'alter table '||quote_ident(shadow_schema_name)||'.'||quote_ident('__shadow_'||tablename)||
		        ' ALTER '||quote_ident(colinfo.attname)||' TYPE '||colinfo.datatype;
	    else
                execute 'alter table '||quote_ident(shadow_schema_name)||'.'||quote_ident('__shadow_'||tablename)||
                        ' ADD COLUMN '||quote_ident(colinfo.attname)||' '||colinfo.datatype;  
	    end if;
        end loop;
        return modifications;
    end 
$$
language plpgsql security definer volatile;

create function shadow_meta.ensure_base_version(_schema text, tname text) returns void as $$
  declare
      shadow_schema_name text;
  begin
      shadow_schema_name = 'shadow_'||_schema;
      execute 'insert into '||quote_ident(shadow_schema_name)||'.'||
               quote_ident('__shadow_'||tname)||' select * from '||quote_ident(_schema)||'.'||quote_ident(tname); 
  end
$$
language plpgsql security definer volatile set client_min_messages = warning;

create function shadow_meta.update_shadow_schema(_for_schema text) returns void as $$
    declare
        tname text;
        modifications boolean;
    begin
        perform shadow_meta.ensure_shadow_schema(_for_schema);
        for tname in (select table_name
                            from information_schema.tables
                           where table_schema = _for_schema and  table_type = 'BASE TABLE' and table_name not in
                                 (select st.tablename from shadow_meta.skip_tables st where st.schemaname = _for_schema)) loop
            if exists (select 1 from information_schema.tables t
                        where t.table_schema = quote_ident('shadow_'||_for_schema)
                              and t.table_name = '__shadow_' || tname) then
                select shadow_meta.modify_table(_for_schema, tname) into modifications;
            else
                perform shadow_meta.create_shadow_table(_for_schema, tname);
                perform shadow_meta.ensure_base_version(_for_schema, tname);
                modifications = true;
            end if;
            if modifications then
               perform shadow_meta.create_triggers(_for_schema, tname);
            end if;
            perform shadow_meta.create_view(_for_schema, tname);
        end loop;
    end 
$$
language plpgsql security definer volatile set client_min_messages = warning;

create function shadow_meta.timetravel(in_schema text, to_time timestamptz) returns void as $$
    declare
        updated integer;
    begin
	execute 'create temp table if not exists timetravel(id integer not null check(id = 1) unique, dest timestamptz)';
	update timetravel set dest = to_time returning id into updated;
	if updated is null then
	    insert into timetravel values(1, to_time);
        end if;
	execute 'set local search_path to shadow_' || in_schema || ', ' || in_schema;
    end
$$
language plpgsql set client_min_messages to warning;
commit;
