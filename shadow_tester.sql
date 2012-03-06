create table public.foobar2 (
    id text not null,
    somecol decimal(10, 5) not null,
    mycol text[],
    primary key (id, somecol)
);
begin;
-- Note: the ensure_shadow_schema('schemaname', true);
-- RECREATES the schema. This means the schema is DROPPED!
-- And this means your precisous edit history is gone!
-- So, use only in testing.
select shadow_meta.ensure_shadow_schema('public', true);
select shadow_meta.update_shadow_schema('public');
commit;
begin;
-- Some testing inside one transaction
insert into public.foobar2 values('1', '1', ARRAY['1']);
select * from shadow_public.__shadow_foobar2;
update public.foobar2 set mycol = null where id = '1' and somecol = '1';
select * from shadow_public.__shadow_foobar2;
set local test_session_var.view_time to '2011-01-01';
select * from shadow_public.foobar2;
set local test_session_var.view_time to '2012-04-01';
select * from shadow_public.foobar2;
delete from public.foobar2 where id = '1' and somecol = '1';
select * from shadow_public.__shadow_foobar2;
commit;
-- And outside transaction
insert into public.foobar2 values('1', '1', ARRAY['1']);
update public.foobar2 set mycol = null where id = '1' and somecol = '1';
select * from shadow_public.__shadow_foobar2;
delete from public.foobar2 where id = '1' and somecol = '1';
select * from shadow_public.__shadow_foobar2;
insert into public.foobar2 values('2', '1', ARRAY['1']);
select * from shadow_public.__shadow_foobar2;
begin;
alter table public.foobar2 add column mynewcol text;
create table public.foobar(id integer primary key, somecol text);
select shadow_meta.update_shadow_schema('public');
commit;
insert into public.foobar values(1, '1');
insert into public.foobar2 values('3', '1', null, 'foof');
select * from shadow_public.__shadow_foobar;
select * from shadow_public.__shadow_foobar2;

drop table if exists public.foobar cascade;
drop table if exists public.foobar2 cascade;
