-- otra: generator-based durable execution on Postgres, built on durable
-- promises.  "¡Otra!" is what the crowd shouts for an encore -- again! -- which
-- is exactly how recovery works here: an execution is re-run from the
-- beginning and fast-forwards through its memoized promise history.
--
-- Design notes
-- ------------
-- The unit of work is an *execution* of a registered durable function.  Every
-- suspension point in a function (a checkpointed side effect, a timer, an
-- event wait, a child call) is represented as a *durable promise*: a
-- write-once register addressed by a deterministic key inside the owning
-- execution's history.  Executions form a tree via parent/child promises.
--
-- Workers claim runnable executions and replay them.  When a function blocks
-- on unresolved remote promises it suspends (releases its claim and consumes
-- nothing).  Resolving a promise (event emitted, timer due, child settled)
-- wakes the owning execution atomically.
--
-- All of the coordination logic lives here, in stored functions; SDKs only
-- ever call these functions.  This mirrors the philosophy of absurd
-- (sql/absurd.sql in this repository), from which this file borrows liberally.

create schema if not exists otra;

------------------------------------------------------------------------------
-- meta + test support
------------------------------------------------------------------------------

create table if not exists otra.config (
  key text primary key,
  value text not null
);

-- Current time, overridable for tests.  Stored in a table (rather than a GUC)
-- so that fake time is visible across all connections of a pool.
create or replace function otra.now () returns timestamptz as $$
  select coalesce(
    (select value::timestamptz from otra.config where key = 'fake_now'),
    clock_timestamp()
  );
$$ language sql stable;

create or replace function otra.set_fake_now (p_now timestamptz) returns void as $$
  insert into otra.config (key, value) values ('fake_now', p_now::text)
  on conflict (key) do update set value = excluded.value;
$$ language sql;

create or replace function otra.advance_fake_now (p_delta interval) returns timestamptz as $$
  update otra.config
     set value = ((value::timestamptz) + p_delta)::text
   where key = 'fake_now'
  returning value::timestamptz;
$$ language sql;

create or replace function otra.clear_fake_now () returns void as $$
  delete from otra.config where key = 'fake_now';
$$ language sql;

-- UUIDv7 (time ordered), generated against otra.now() so partition routing and
-- deterministic tests use the same clock on every supported PostgreSQL.
create or replace function otra.uuid_v7 () returns uuid as $$
declare
  v_ts_ms bigint := floor(extract(epoch from otra.now()) * 1000)::bigint;
  v_bytes bytea := repeat(E'\\000', 16)::bytea;
  v_random bytea := uuid_send(gen_random_uuid());
  i int;
begin
  if v_ts_ms < 0 or v_ts_ms > 281474976710655 then
    raise exception 'Timestamp "%" is outside UUIDv7 supported range', otra.now();
  end if;
  for i in 0..5 loop
    v_bytes := set_byte(v_bytes, i, ((v_ts_ms >> ((5 - i) * 8)) & 255)::int);
  end loop;
  for i in 6..15 loop
    v_bytes := set_byte(v_bytes, i, get_byte(v_random, i));
  end loop;
  v_bytes := set_byte(v_bytes, 6, ((get_byte(v_bytes, 6) & 15) | (7 << 4)));
  v_bytes := set_byte(v_bytes, 8, ((get_byte(v_bytes, 8) & 63) | 128));
  return encode(v_bytes, 'hex')::uuid;
end;
$$ language plpgsql volatile;

create or replace function otra.uuid_v7_timestamp (p_id uuid) returns timestamptz as $$
  with bytes as (
    select uuid_send(p_id) as value
  ), decoded as (
    select
      get_byte(value, 6) >> 4 as version,
      ((get_byte(value, 0)::bigint << 40) |
       (get_byte(value, 1)::bigint << 32) |
       (get_byte(value, 2)::bigint << 24) |
       (get_byte(value, 3)::bigint << 16) |
       (get_byte(value, 4)::bigint << 8) |
        get_byte(value, 5)::bigint) as timestamp_ms
    from bytes
  )
  select case when version = 7
    then 'epoch'::timestamptz + timestamp_ms * interval '1 millisecond'
    else null
  end
  from decoded;
$$ language sql immutable strict;

create or replace function otra.uuid_v7_floor (p_time timestamptz) returns uuid as $$
declare
  v_ts_ms bigint := floor(extract(epoch from p_time) * 1000)::bigint;
  v_bytes bytea := repeat(E'\\000', 16)::bytea;
  i int;
begin
  if v_ts_ms < 0 or v_ts_ms > 281474976710655 then
    raise exception 'Timestamp "%" is outside UUIDv7 supported range', p_time;
  end if;
  for i in 0..5 loop
    v_bytes := set_byte(v_bytes, i, ((v_ts_ms >> ((5 - i) * 8)) & 255)::int);
  end loop;
  v_bytes := set_byte(v_bytes, 6, (7 << 4));
  v_bytes := set_byte(v_bytes, 8, 128);
  return encode(v_bytes, 'hex')::uuid;
end;
$$ language plpgsql immutable strict;

create or replace function otra.week_bucket_utc (p_time timestamptz) returns timestamptz as $$
  select date_trunc('week', p_time at time zone 'UTC') at time zone 'UTC';
$$ language sql immutable strict;

create or replace function otra.partition_week_tag (p_time timestamptz) returns text as $$
  select to_char(otra.week_bucket_utc(p_time) at time zone 'UTC', 'IYYYIW');
$$ language sql immutable strict;

------------------------------------------------------------------------------
-- tables
------------------------------------------------------------------------------

create table if not exists otra.queues (
  id           uuid primary key default otra.uuid_v7(),
  name         text not null unique,
  storage_mode text not null default 'unpartitioned',
  default_partition text not null default 'enabled',
  partition_lookahead interval not null default interval '28 days',
  partition_lookback interval not null default interval '1 day',
  cleanup_ttl interval not null default interval '30 days',
  cleanup_limit int not null default 1000,
  detach_mode text not null default 'none',
  detach_min_age interval not null default interval '30 days',
  created_at   timestamptz not null default otra.now(),
  constraint queues_storage_mode_check check (
    storage_mode in ('unpartitioned', 'partitioned')
  ),
  constraint queues_default_partition_check check (
    default_partition in ('enabled', 'disabled')
  ),
  constraint queues_partition_lookahead_check check (
    partition_lookahead >= interval '0 seconds'
  ),
  constraint queues_partition_lookback_check check (
    partition_lookback >= interval '0 seconds'
  ),
  constraint queues_cleanup_ttl_check check (
    cleanup_ttl >= interval '0 seconds'
  ),
  constraint queues_cleanup_limit_check check (
    cleanup_limit >= 1
  ),
  constraint queues_detach_mode_check check (
    detach_mode in ('none', 'empty')
  ),
  constraint queues_detach_min_age_check check (
    detach_min_age >= interval '0 seconds'
  )
);

create or replace function otra._protect_queue_storage_identity ()
returns trigger as $$
begin
  if new.id is distinct from old.id
     or new.storage_mode is distinct from old.storage_mode then
    raise exception 'queue storage identity is immutable';
  end if;
  return new;
end;
$$ language plpgsql;

drop trigger if exists queues_storage_identity_guard on otra.queues;
create trigger queues_storage_identity_guard
before update on otra.queues
for each row execute function otra._protect_queue_storage_identity();

------------------------------------------------------------------------------
-- queue provisioning
------------------------------------------------------------------------------

-- Queue names remain compact routing labels; physical identifiers derive from
-- the queue UUID so labels cannot collide with generated relations.
create or replace function otra.validate_queue_name (p_name text) returns text as $$
begin
  if p_name is null or p_name = '' then
    raise exception 'Queue name must be provided';
  end if;
  if octet_length(p_name) > 57 then
    raise exception 'Queue name "%" is too long (max 57 bytes).', p_name;
  end if;
  return p_name;
end;
$$ language plpgsql immutable;

drop function if exists otra._ensure_queue_tables (text);
create or replace function otra._ensure_queue_tables (p_queue uuid) returns void as $$
declare
  v_storage text := replace(p_queue::text, '-', '');
  v_x text := 'x_' || v_storage;
  v_p text := 'p_' || v_storage;
  v_e text := 'e_' || v_storage;
  v_i text := 'i_' || v_storage;
  v_mode text;
  v_partition_suffix text := '';
begin
  select storage_mode into v_mode from otra.queues where id = p_queue;
  if v_mode is null then
    raise exception 'Queue % is not provisioned', p_queue;
  end if;
  if v_mode = 'partitioned' then
    v_partition_suffix := 'partition by range (root_id)';
  end if;

  execute format(
    'create table if not exists otra.%I (
       id                  uuid not null default otra.uuid_v7(),
       function_name       text not null,
       params              jsonb,
       status              text not null default ''pending'',
       attempt             int not null default 0,
       max_attempts        int not null default 5,
       retry_strategy      jsonb not null default ''{"kind": "exponential", "base_s": 1, "factor": 2, "max_s": 300}'',
       run_after           timestamptz not null default otra.now(),
       claimed_by          text,
       claim_expires_at    timestamptz,
       root_id             uuid not null,
       parent_id           uuid,
       result              jsonb,
       error               jsonb,
       idempotency_key     text,
       cancel_requested_at timestamptz,
       cancel_reason       text,
       on_parent_cancel    text not null default ''cascade'',
       created_at          timestamptz not null default otra.now(),
       updated_at          timestamptz not null default otra.now(),
       finished_at         timestamptz,
       primary key (root_id, id),
       foreign key (root_id, parent_id) references otra.%I (root_id, id) on delete cascade,
       check (status in (''pending'', ''running'', ''suspended'', ''completed'', ''failed'', ''cancelled'')),
       check (on_parent_cancel in (''cascade'', ''detach'')),
       check (parent_id is not null or root_id = id)
     ) %s',
    v_x,
    v_x,
    v_partition_suffix
  );

  execute format(
    'create table if not exists otra.%I (
       id                 uuid not null default otra.uuid_v7(),
       root_id            uuid not null,
       execution_id       uuid not null,
       key                text not null,
       label              text not null,
       kind               text not null,
       status             text not null default ''pending'',
       value              jsonb,
       error              jsonb,
       wake_at            timestamptz,
       event_name         text,
       child_execution_id uuid,
       created_at         timestamptz not null default otra.now(),
       settled_at         timestamptz,
       primary key (root_id, id),
       foreign key (root_id, execution_id) references otra.%I (root_id, id) on delete cascade,
       foreign key (root_id, child_execution_id) references otra.%I (root_id, id) on delete cascade,
       check (kind in (''run'', ''sleep'', ''event'', ''child'', ''external'', ''cancel'')),
       check (status in (''pending'', ''resolved'', ''rejected'')),
       unique (root_id, execution_id, key)
     ) %s',
    v_p,
    v_x,
    v_x,
    v_partition_suffix
  );

  execute format(
    'create table if not exists otra.%I (
       id         uuid primary key default otra.uuid_v7(),
       name       text not null unique,
       payload    jsonb,
       created_at timestamptz not null default otra.now()
     )',
    v_e
  );

  if v_mode = 'partitioned' then
    execute format(
      'create table if not exists otra.%I (
         idempotency_key text primary key,
         root_id uuid not null,
         execution_id uuid not null
       )',
      v_i
    );
  end if;

  execute format(
    'create index if not exists %I on otra.%I (run_after) where status = ''pending''',
    'xi_' || v_storage || '_ri',
    v_x
  );
  execute format(
    'create index if not exists %I on otra.%I (claim_expires_at) where status = ''running''',
    'xi_' || v_storage || '_cei',
    v_x
  );
  execute format(
    'create index if not exists %I on otra.%I (parent_id) where parent_id is not null',
    'xi_' || v_storage || '_pi',
    v_x
  );
  if v_mode = 'unpartitioned' then
    execute format(
      'create unique index if not exists %I on otra.%I (idempotency_key) where idempotency_key is not null',
      'xi_' || v_storage || '_ii',
      v_x
    );
  end if;
  execute format(
    'create index if not exists %I on otra.%I (wake_at) where status = ''pending'' and wake_at is not null',
    'pi_' || v_storage || '_wi',
    v_p
  );
  execute format(
    'create index if not exists %I on otra.%I (event_name) where status = ''pending'' and kind = ''event''',
    'pi_' || v_storage || '_ei',
    v_p
  );
  execute format(
    'create index if not exists %I on otra.%I (child_execution_id) where kind = ''child''',
    'pi_' || v_storage || '_ci',
    v_p
  );
  -- cleanup_local's candidate scan: terminal roots ordered by finished_at.
  execute format(
    'create index if not exists %I on otra.%I (finished_at)
      where root_id = id and status in (''completed'', ''failed'', ''cancelled'')',
    'xi_' || v_storage || '_fin',
    v_x
  );
  -- cleanup_local's event-TTL batch orders by created_at.
  execute format(
    'create index if not exists %I on otra.%I (created_at)',
    'ei_' || v_storage || '_ci',
    v_e
  );
  if v_mode = 'partitioned' then
    -- cleanup_local deletes idempotency registrations by root.
    execute format(
      'create index if not exists %I on otra.%I (root_id)',
      'ii_' || v_storage || '_ri',
      v_i
    );
  end if;
end;
$$ language plpgsql;

-- Provision one week window for a queue's execution+promise pair.  Handles
-- the default-partition wedge: rows that landed in the default while
-- maintenance lapsed are DRAINED into the new week partition before it is
-- attached (plain CREATE ... PARTITION OF fails permanently against a
-- non-empty default -- Postgres re-validates the default's constraint).
-- Runs under the caller's FOR UPDATE queue barrier, so no spawn/claim
-- (FOR KEY SHARE) can write concurrently.  Move order is FK-driven: promise
-- rows in range are parked in a temp table and their originals deleted
-- FIRST, so moving execution rows (delete + reinsert) cannot cascade away
-- live history; executions attach before promises so the promise reinsert
-- routes against visible referents.
create or replace function otra._ensure_week_partitions (
  p_x text, p_p text, p_tag text, p_lower uuid, p_upper uuid
) returns void as $$
declare
  v_x_part text := p_x || '_' || p_tag;
  v_p_part text := p_p || '_' || p_tag;
  v_need_x boolean := to_regclass('otra.' || v_x_part) is null;
  v_need_p boolean := to_regclass('otra.' || v_p_part) is null;
  v_x_d text := p_x || '_d';
  v_p_d text := p_p || '_d';
  v_stranded boolean := false;
begin
  if not v_need_x and not v_need_p then
    return;
  end if;

  if to_regclass('otra.' || v_x_d) is not null then
    execute format(
      'select exists (select 1 from otra.%I where root_id >= %L::uuid and root_id < %L::uuid)',
      v_x_d, p_lower, p_upper
    ) into v_stranded;
  end if;
  if not v_stranded and to_regclass('otra.' || v_p_d) is not null then
    execute format(
      'select exists (select 1 from otra.%I where root_id >= %L::uuid and root_id < %L::uuid)',
      v_p_d, p_lower, p_upper
    ) into v_stranded;
  end if;

  if not v_stranded then
    -- Fast path: an empty (or absent) default validates instantly.
    if v_need_x then
      execute format(
        'create table if not exists otra.%I partition of otra.%I
         for values from (%L::uuid) to (%L::uuid)',
        v_x_part, p_x, p_lower, p_upper
      );
    end if;
    if v_need_p then
      execute format(
        'create table if not exists otra.%I partition of otra.%I
         for values from (%L::uuid) to (%L::uuid)',
        v_p_part, p_p, p_lower, p_upper
      );
    end if;
    return;
  end if;

  -- Park every promise row of the affected trees (reading through the
  -- parent covers both the default and any attached partition) and delete
  -- the originals, so the execution move below cannot cascade them away.
  execute format(
    'create temp table otra_drain_p on commit drop as
     select * from otra.%I where root_id >= %L::uuid and root_id < %L::uuid',
    p_p, p_lower, p_upper
  );
  execute format(
    'delete from otra.%I where root_id >= %L::uuid and root_id < %L::uuid',
    p_p, p_lower, p_upper
  );

  if v_need_x then
    execute format(
      'create table otra.%I (like otra.%I including all)', v_x_part, p_x
    );
    execute format(
      'insert into otra.%I select * from otra.%I
        where root_id >= %L::uuid and root_id < %L::uuid',
      v_x_part, v_x_d, p_lower, p_upper
    );
    execute format(
      'delete from otra.%I where root_id >= %L::uuid and root_id < %L::uuid',
      v_x_d, p_lower, p_upper
    );
    execute format(
      'alter table otra.%I attach partition otra.%I
       for values from (%L::uuid) to (%L::uuid)',
      p_x, v_x_part, p_lower, p_upper
    );
  end if;

  if v_need_p then
    execute format(
      'create table if not exists otra.%I partition of otra.%I
       for values from (%L::uuid) to (%L::uuid)',
      v_p_part, p_p, p_lower, p_upper
    );
  end if;
  execute format('insert into otra.%I select * from otra_drain_p', p_p);
  drop table otra_drain_p;
end;
$$ language plpgsql;

-- Provision the partition window for one queue.  A cheap unlocked probe
-- skips fully-provisioned queues without touching the queue barrier (the
-- common no-op run must not block spawn/claim); otherwise the barrier is
-- taken and the policy re-read under it.
create or replace function otra._ensure_queue_partitions (p_queue uuid) returns void as $$
declare
  v_row otra.queues%rowtype;
  v_now timestamptz;
  v_start timestamptz;
  v_end timestamptz;
  v_week timestamptz;
  v_next timestamptz;
  v_storage text;
  v_x text;
  v_p text;
  v_missing boolean := false;
begin
  select * into v_row from otra.queues where id = p_queue;
  if not found or v_row.storage_mode <> 'partitioned' then
    return;
  end if;
  v_storage := replace(p_queue::text, '-', '');
  v_x := 'x_' || v_storage;
  v_p := 'p_' || v_storage;
  v_now := otra.now();
  v_start := otra.week_bucket_utc(v_now - v_row.partition_lookback);
  v_end := otra.week_bucket_utc(v_now + v_row.partition_lookahead);

  if v_row.default_partition = 'enabled'
     and (to_regclass('otra.' || v_x || '_d') is null
          or to_regclass('otra.' || v_p || '_d') is null) then
    v_missing := true;
  end if;
  v_week := v_start;
  while not v_missing and v_week <= v_end loop
    if to_regclass('otra.' || v_x || '_' || otra.partition_week_tag(v_week)) is null
       or to_regclass('otra.' || v_p || '_' || otra.partition_week_tag(v_week)) is null then
      v_missing := true;
    end if;
    -- Stepping through week_bucket_utc keeps the walk in UTC: plain
    -- "+ interval '7 days'" is evaluated in the session time zone, and a
    -- DST spring-forward makes it land inside the SAME ISO week -- the next
    -- window then gets the same tag and a whole week goes uncovered.
    v_week := otra.week_bucket_utc(v_week + interval '8 days');
  end loop;
  if not v_missing then
    return;
  end if;

  -- Re-read under the maintenance barrier; spawn/claim (FOR KEY SHARE)
  -- block for this queue only while real DDL work happens.
  select * into v_row from otra.queues where id = p_queue for update;

  if v_row.default_partition = 'enabled' then
    execute format(
      'create table if not exists otra.%I partition of otra.%I default',
      v_x || '_d', v_x
    );
    execute format(
      'create table if not exists otra.%I partition of otra.%I default',
      v_p || '_d', v_p
    );
  end if;

  v_start := otra.week_bucket_utc(v_now - v_row.partition_lookback);
  v_end := otra.week_bucket_utc(v_now + v_row.partition_lookahead);
  v_week := v_start;
  while v_week <= v_end loop
    v_next := otra.week_bucket_utc(v_week + interval '8 days');
    perform otra._ensure_week_partitions(
      v_x, v_p, otra.partition_week_tag(v_week),
      otra.uuid_v7_floor(v_week), otra.uuid_v7_floor(v_next)
    );
    v_week := v_next;
  end loop;
end;
$$ language plpgsql;

-- Provision partition windows.  With a name: that queue, loudly.  With no
-- name: every partitioned queue, each isolated in its own subtransaction so
-- one failing queue cannot poison the whole maintenance run.  NOTE: a
-- multi-queue run still holds each queue's barrier until this transaction
-- commits; schedulers should prefer one call per queue per transaction (the
-- SDK's ensurePartitions() does exactly that).
create or replace function otra.ensure_partitions (p_name text default null) returns void as $$
declare
  v_queue record;
  v_mode text;
begin
  if p_name is not null then
    select storage_mode into v_mode from otra.queues where name = p_name;
    if v_mode is null then
      raise exception 'Queue "%" does not exist', p_name;
    end if;
    if v_mode <> 'partitioned' then
      raise exception 'Queue "%" is not partitioned', p_name;
    end if;
  end if;

  for v_queue in
    select q.id, q.name from otra.queues q
     where q.storage_mode = 'partitioned'
       and (p_name is null or q.name = p_name)
     order by q.id
  loop
    if p_name is not null then
      perform otra._ensure_queue_partitions(v_queue.id);
    else
      begin
        perform otra._ensure_queue_partitions(v_queue.id);
      exception when others then
        raise warning 'otra.ensure_partitions: queue "%" failed: %',
          v_queue.name, sqlerrm;
      end;
    end if;
  end loop;
end;
$$ language plpgsql;

create or replace function otra.create_queue (p_name text, p_storage_mode text) returns void as $$
declare
  v_queue uuid;
  v_mode text := lower(trim(coalesce(p_storage_mode, '')));
  v_existing_mode text;
begin
  p_name := otra.validate_queue_name(p_name);
  if v_mode not in ('unpartitioned', 'partitioned') then
    raise exception 'Unsupported queue storage mode "%"', p_storage_mode;
  end if;
  insert into otra.queues (name, storage_mode) values (p_name, v_mode)
  on conflict (name) do nothing;
  select id, storage_mode into strict v_queue, v_existing_mode
    from otra.queues where name = p_name for update;
  if v_existing_mode <> v_mode then
    raise exception 'Queue "%" already exists with storage mode "%"',
      p_name, v_existing_mode;
  end if;
  perform otra._ensure_queue_tables(v_queue);
  if v_mode = 'partitioned' then
    perform otra.ensure_partitions(p_name);
  end if;
end;
$$ language plpgsql;

create or replace function otra.create_queue (p_name text) returns void as $$
begin
  perform otra.create_queue(p_name, 'unpartitioned');
end;
$$ language plpgsql;

create or replace function otra.get_queue (p_name text)
returns table (name text, storage_mode text) as $$
  select q.name, q.storage_mode from otra.queues q where q.name = p_name;
$$ language sql stable;

create or replace function otra.list_queues ()
returns table (name text, storage_mode text) as $$
  select q.name, q.storage_mode from otra.queues q order by q.name;
$$ language sql stable;

create or replace function otra.get_queue_policy (p_name text)
returns table (
  name text,
  storage_mode text,
  default_partition text,
  partition_lookahead interval,
  partition_lookback interval,
  cleanup_ttl interval,
  cleanup_limit int,
  detach_mode text,
  detach_min_age interval
) as $$
  select q.name, q.storage_mode, q.default_partition,
         q.partition_lookahead, q.partition_lookback,
         q.cleanup_ttl, q.cleanup_limit, q.detach_mode, q.detach_min_age
    from otra.queues q
   where q.name = p_name;
$$ language sql stable;

create or replace function otra.set_queue_policy (p_name text, p_policy jsonb)
returns void as $$
declare
  v_unknown text;
  v_lookahead interval;
  v_lookback interval;
  v_cleanup_ttl interval;
  v_cleanup_limit int;
  v_detach_mode text;
  v_detach_min_age interval;
  v_default_partition text;
  v_previous_default text;
  v_storage_mode text;
  v_queue_id uuid;
  v_storage text;
  v_parent text;
  v_default text;
  v_attached boolean;
  v_has_rows boolean;
  v_prefix text;
begin
  select key into v_unknown
    from jsonb_object_keys(coalesce(p_policy, '{}'::jsonb)) as keys(key)
   where key not in (
     'partition_lookahead', 'partition_lookback', 'cleanup_ttl',
     'cleanup_limit', 'detach_mode', 'detach_min_age', 'default_partition'
   )
   limit 1;
  if v_unknown is not null then
    raise exception 'Unknown queue policy key "%"', v_unknown;
  end if;

  select q.id, q.storage_mode, q.default_partition,
         q.partition_lookahead, q.partition_lookback,
         q.cleanup_ttl, q.cleanup_limit, q.detach_mode, q.detach_min_age
    into v_queue_id, v_storage_mode, v_default_partition,
         v_lookahead, v_lookback, v_cleanup_ttl,
         v_cleanup_limit, v_detach_mode, v_detach_min_age
    from otra.queues q
   where q.name = p_name
     for update;
  if not found then
    raise exception 'Queue "%" does not exist', p_name;
  end if;

  if p_policy ? 'partition_lookahead' then
    v_lookahead := (p_policy ->> 'partition_lookahead')::interval;
  end if;
  if p_policy ? 'partition_lookback' then
    v_lookback := (p_policy ->> 'partition_lookback')::interval;
  end if;
  if p_policy ? 'cleanup_ttl' then
    v_cleanup_ttl := (p_policy ->> 'cleanup_ttl')::interval;
  end if;
  if p_policy ? 'cleanup_limit' then
    v_cleanup_limit := (p_policy ->> 'cleanup_limit')::int;
  end if;
  if p_policy ? 'detach_mode' then
    v_detach_mode := lower(trim(coalesce(p_policy ->> 'detach_mode', '')));
  end if;
  if p_policy ? 'detach_min_age' then
    v_detach_min_age := (p_policy ->> 'detach_min_age')::interval;
  end if;
  v_previous_default := v_default_partition;
  if p_policy ? 'default_partition' then
    v_default_partition := lower(trim(coalesce(p_policy ->> 'default_partition', '')));
  end if;

  if v_lookahead < interval '0 seconds' then
    raise exception 'partition_lookahead must be non-negative';
  end if;
  if v_lookback < interval '0 seconds' then
    raise exception 'partition_lookback must be non-negative';
  end if;
  if v_cleanup_ttl < interval '0 seconds' then
    raise exception 'cleanup_ttl must be non-negative';
  end if;
  if v_cleanup_limit < 1 then
    raise exception 'cleanup_limit must be at least 1';
  end if;
  if v_detach_mode not in ('none', 'empty') then
    raise exception 'Unsupported detach mode "%"', v_detach_mode;
  end if;
  if v_detach_min_age < interval '0 seconds' then
    raise exception 'detach_min_age must be non-negative';
  end if;
  if v_default_partition not in ('enabled', 'disabled') then
    raise exception 'Unsupported default_partition mode "%"', v_default_partition;
  end if;
  if v_storage_mode <> 'partitioned' and p_policy ? 'default_partition' then
    raise exception 'default_partition policy is only supported for partitioned queues';
  end if;

  update otra.queues
     set default_partition = v_default_partition,
         partition_lookahead = v_lookahead,
         partition_lookback = v_lookback,
         cleanup_ttl = v_cleanup_ttl,
         cleanup_limit = v_cleanup_limit,
         detach_mode = v_detach_mode,
         detach_min_age = v_detach_min_age
   where name = p_name;

  if v_storage_mode = 'partitioned'
     and v_previous_default <> v_default_partition then
    if v_default_partition = 'enabled' then
      perform otra.ensure_partitions(p_name);
    else
      v_storage := replace(v_queue_id::text, '-', '');
      -- Partition maintenance takes parent locks before leaf locks. Queue-local
      -- coordination takes a compatible lock on the queue row before touching
      -- either family, so policy changes form a maintenance barrier.
      execute format(
        'lock table otra.%I in access exclusive mode',
        'x_' || v_storage
      );
      execute format(
        'lock table otra.%I in access exclusive mode',
        'p_' || v_storage
      );
      foreach v_prefix in array array['p', 'x'] loop
        v_parent := v_prefix || '_' || v_storage;
        v_default := v_parent || '_d';
        select exists (
          select 1
            from pg_inherits i
            join pg_class parent on parent.oid = i.inhparent
            join pg_class child on child.oid = i.inhrelid
            join pg_namespace n on n.oid = parent.relnamespace
           where n.nspname = 'otra'
             and parent.relname = v_parent
             and child.relname = v_default
        ) into v_attached;
        if not v_attached then
          continue;
        end if;
        execute format('lock table otra.%I in access exclusive mode', v_default);
        execute format('select exists (select 1 from otra.%I limit 1)', v_default)
          into v_has_rows;
        if v_has_rows then
          raise exception
            'Cannot disable default_partition for queue "%": default partition "%" is not empty',
            p_name, v_default;
        end if;
        execute format(
          'alter table otra.%I detach partition otra.%I',
          v_parent, v_default
        );
        execute format('drop table otra.%I', v_default);
      end loop;
    end if;
  end if;
end;
$$ language plpgsql;

-- Discover old empty partitions. Detach itself remains a top-level operator
-- action because PostgreSQL cannot run DETACH PARTITION CONCURRENTLY here.
create or replace function otra.list_detach_candidates (p_name text default null)
returns table (queue_name text, parent_table text, partition_table text) as $$
declare
  v_now timestamptz := otra.now();
  v_queue record;
  v_x_parent text;
  v_p_parent text;
  v_x_parent_oid oid;
  v_p_parent_oid oid;
  v_partition record;
  v_p_partition text;
  v_suffix text;
  v_upper uuid;
  v_upper_at timestamptz;
  v_x_has_rows boolean;
  v_p_has_rows boolean;
  v_p_attached boolean;
  v_storage text;
begin
  if p_name is not null and not exists (select 1 from otra.queues where name = p_name) then
    raise exception 'Queue "%" does not exist', p_name;
  end if;

  for v_queue in
    select q.id, q.name, q.detach_min_age
      from otra.queues q
     where q.storage_mode = 'partitioned'
       and q.detach_mode = 'empty'
       and (p_name is null or q.name = p_name)
     order by q.name
  loop
    v_storage := replace(v_queue.id::text, '-', '');
    v_x_parent := 'x_' || v_storage;
    v_p_parent := 'p_' || v_storage;
    select c.oid into v_x_parent_oid
      from pg_class c
      join pg_namespace n on n.oid = c.relnamespace
     where n.nspname = 'otra' and c.relname = v_x_parent;
    select c.oid into v_p_parent_oid
      from pg_class c
      join pg_namespace n on n.oid = c.relnamespace
     where n.nspname = 'otra' and c.relname = v_p_parent;
    if v_x_parent_oid is null or v_p_parent_oid is null then
      continue;
    end if;

    for v_partition in
      select child.relname as name,
             pg_get_expr(child.relpartbound, child.oid) as bound
        from pg_inherits i
        join pg_class child on child.oid = i.inhrelid
       where i.inhparent = v_x_parent_oid
    loop
      if v_partition.bound = 'DEFAULT' then
        continue;
      end if;
      select (regexp_match(
        v_partition.bound,
        'TO \(''([^'']+)''(::uuid)?\)'
      ))[1]::uuid into v_upper;
      if v_upper is null then
        continue;
      end if;
      v_upper_at := otra.uuid_v7_timestamp(v_upper);
      if v_upper_at is null
         or v_upper_at >= v_now - v_queue.detach_min_age then
        continue;
      end if;

      v_suffix := substring(v_partition.name from length(v_x_parent) + 1);
      v_p_partition := v_p_parent || v_suffix;
      select exists (
        select 1
          from pg_inherits i
          join pg_class child on child.oid = i.inhrelid
         where i.inhparent = v_p_parent_oid
           and child.relname = v_p_partition
      ) into v_p_attached;
      if not v_p_attached then
        continue;
      end if;

      execute format(
        'select exists (select 1 from otra.%I limit 1)',
        v_partition.name
      ) into v_x_has_rows;
      execute format(
        'select exists (select 1 from otra.%I limit 1)',
        v_p_partition
      ) into v_p_has_rows;
      if v_x_has_rows or v_p_has_rows then
        continue;
      end if;

      queue_name := v_queue.name;
      parent_table := v_p_parent;
      partition_table := v_p_partition;
      return next;
      queue_name := v_queue.name;
      parent_table := v_x_parent;
      partition_table := v_partition.name;
      return next;
    end loop;
  end loop;
end;
$$ language plpgsql;

------------------------------------------------------------------------------
-- internal helpers
------------------------------------------------------------------------------

-- Compute (and validate) the retry backoff.  Validation raises OT003 so
-- spawn can reject a bad strategy up front instead of letting it poison the
-- claim sweep later (absurd's retry_delay_seconds lesson, commit 866480d).
-- Delays are hard-capped at one day no matter what the strategy asks for.
create or replace function otra._backoff (p_strategy jsonb, p_attempt int) returns interval as $$
declare
  v_kind text;
  v_base double precision;
  v_factor double precision;
  v_max double precision;
  v_delay double precision;
begin
  if p_strategy is null or jsonb_typeof(p_strategy) <> 'object' then
    raise exception 'retry_strategy must be a JSON object'
      using errcode = 'OT003';
  end if;
  v_kind := coalesce(p_strategy ->> 'kind', 'exponential');
  if v_kind not in ('fixed', 'exponential') then
    raise exception 'unknown retry strategy kind: %', v_kind
      using errcode = 'OT003';
  end if;
  begin
    v_base := coalesce((p_strategy ->> 'base_s')::double precision, 1);
    v_factor := coalesce((p_strategy ->> 'factor')::double precision, 2);
    v_max := coalesce((p_strategy ->> 'max_s')::double precision, 300);
  exception when others then
    raise exception 'retry_strategy has non-numeric fields'
      using errcode = 'OT003';
  end;
  -- Comparisons are written so that NaN fails them too.
  if not (v_base >= 0 and v_base <= 86400) then
    raise exception 'retry_strategy base_s must be within [0, 86400]'
      using errcode = 'OT003';
  end if;
  if not (v_factor >= 1 and v_factor <= 1000) then
    raise exception 'retry_strategy factor must be within [1, 1000]'
      using errcode = 'OT003';
  end if;
  if not (v_max >= 0) then
    raise exception 'retry_strategy max_s must be >= 0'
      using errcode = 'OT003';
  end if;
  v_max := least(v_max, 86400);
  if v_max = 0 then
    return interval '0';
  end if;

  if v_kind = 'fixed' then
    v_delay := v_base;
  elsif v_base = 0 or v_factor = 1 then
    v_delay := v_base;
  elsif ln(v_base) + greatest(p_attempt - 1, 0) * ln(v_factor) >= ln(v_max) then
    -- Saturate before calling power(): float8 power() raises "value out of
    -- range" long before the least() cap could apply (absurd hit this same
    -- overflow in retry_delay_seconds).
    v_delay := v_max;
  else
    v_delay := v_base * power(v_factor, greatest(p_attempt - 1, 0));
  end if;
  return make_interval(secs => least(v_delay, v_max));
end;
$$ language plpgsql immutable;

------------------------------------------------------------------------------
-- public API
------------------------------------------------------------------------------

-- Queue-local top-level spawn. The execution address returned here carries
-- every key needed for direct, partition-pruned coordination.
create or replace function otra.spawn_local (
  p_function text,
  p_params jsonb,
  p_queue text,
  p_opts jsonb default '{}'
) returns table (
  queue_id uuid,
  root_id uuid,
  execution_id uuid,
  created boolean
) as $$
declare
  v_queue_id uuid;
  v_mode text;
  v_storage text;
  v_x text;
  v_i text;
  v_id uuid := otra.uuid_v7();
  v_root uuid;
  v_existing uuid;
  v_inserted boolean;
  v_delay double precision := coalesce((p_opts ->> 'delay_s')::double precision, 0);
  v_idempotency_key text := p_opts ->> 'idempotency_key';
  v_strategy jsonb := coalesce(
    p_opts -> 'retry_strategy',
    '{"kind": "exponential", "base_s": 1, "factor": 2, "max_s": 300}'::jsonb
  );
begin
  perform otra._backoff(v_strategy, 1);
  select q.id, q.storage_mode into v_queue_id, v_mode
    from otra.queues q
   where q.name = p_queue
     for key share;
  if not found then
    raise exception 'Queue "%" does not exist', p_queue;
  end if;

  v_storage := replace(v_queue_id::text, '-', '');
  v_x := 'x_' || v_storage;
  v_i := 'i_' || v_storage;

  if v_idempotency_key is not null and v_mode = 'partitioned' then
    execute format(
      'insert into otra.%I (idempotency_key, root_id, execution_id)
       values ($1, $2, $2)
       on conflict (idempotency_key) do nothing
       returning true',
      v_i
    ) into v_inserted using v_idempotency_key, v_id;

    if coalesce(v_inserted, false) then
      v_root := v_id;
      v_existing := v_id;
    else
      execute format(
        'select i.root_id, i.execution_id
           from otra.%1$I i
           join otra.%2$I x
             on x.root_id = i.root_id and x.id = i.execution_id
          where i.idempotency_key = $1
            for key share of i',
        v_i, v_x
      ) into v_root, v_existing using v_idempotency_key;
      if v_existing is null then
        raise exception 'concurrent idempotent spawn was cleaned up; retry'
          using errcode = '40001';
      end if;
      return query select v_queue_id, v_root, v_existing, false;
      return;
    end if;
  end if;

  if v_existing is null then
    if v_idempotency_key is not null then
      execute format(
        'insert into otra.%I
           (id, root_id, function_name, params, max_attempts, retry_strategy,
            run_after, idempotency_key, on_parent_cancel)
         values ($1, $1, $2, $3, $4, $5, $6, $7, $8)
         on conflict (idempotency_key) where idempotency_key is not null do nothing
         returning id',
        v_x
      ) into v_existing using
        v_id, p_function, p_params,
        coalesce((p_opts ->> 'max_attempts')::int, 5),
        v_strategy, otra.now() + make_interval(secs => v_delay),
        v_idempotency_key,
        coalesce(p_opts ->> 'on_parent_cancel', 'cascade');
      if v_existing is null then
        execute format(
          'select id from otra.%I where idempotency_key = $1 for key share',
          v_x
        ) into v_existing using v_idempotency_key;
        if v_existing is null then
          raise exception 'concurrent idempotent spawn aborted; retry'
            using errcode = '40001';
        end if;
        return query select v_queue_id, v_existing, v_existing, false;
        return;
      end if;
    else
      execute format(
        'insert into otra.%I
           (id, root_id, function_name, params, max_attempts, retry_strategy,
            run_after, on_parent_cancel)
         values ($1, $1, $2, $3, $4, $5, $6, $7)
         returning id',
        v_x
      ) into v_existing using
        v_id, p_function, p_params,
        coalesce((p_opts ->> 'max_attempts')::int, 5),
        v_strategy, otra.now() + make_interval(secs => v_delay),
        coalesce(p_opts ->> 'on_parent_cancel', 'cascade');
    end if;
    v_root := v_existing;
  else
    execute format(
      'insert into otra.%I
         (id, root_id, function_name, params, max_attempts, retry_strategy,
          run_after, idempotency_key, on_parent_cancel)
       values ($1, $1, $2, $3, $4, $5, $6, $7, $8)',
      v_x
    ) using
      v_existing, p_function, p_params,
      coalesce((p_opts ->> 'max_attempts')::int, 5),
      v_strategy, otra.now() + make_interval(secs => v_delay),
      v_idempotency_key,
      coalesce(p_opts ->> 'on_parent_cancel', 'cascade');
  end if;

  perform pg_notify('otra_wake', p_queue);
  return query select v_queue_id, v_root, v_existing, true;
end;
$$ language plpgsql;

create or replace function otra.claim_local (
  p_queue text,
  p_worker text,
  p_claim_seconds double precision default 30,
  p_batch int default 1
) returns table (
  queue_id uuid,
  root_id uuid,
  execution_id uuid,
  function_name text,
  params jsonb,
  attempt int,
  max_attempts int,
  cancel_requested boolean
) as $$
declare
  v_queue_id uuid;
  v_storage text;
  v_x text;
  v_p text;
  v_now timestamptz := otra.now();
  v_woken record;
  v_crashed record;
begin
  if p_claim_seconds is null or p_claim_seconds <= 0 then
    raise exception 'claim lease must be positive, got %', p_claim_seconds
      using errcode = 'OT003';
  end if;
  select q.id into v_queue_id
    from otra.queues q
   where q.name = p_queue
     for key share;
  if not found then
    raise exception 'Queue "%" does not exist', p_queue;
  end if;
  v_storage := replace(v_queue_id::text, '-', '');
  v_x := 'x_' || v_storage;
  v_p := 'p_' || v_storage;

  for v_woken in execute format(
    'with due as (
       select root_id, id from otra.%I
        where kind = ''sleep'' and status = ''pending'' and wake_at <= $1
        order by wake_at, id limit 100 for update skip locked
     ), fired as (
       update otra.%I p
          set status = ''resolved'', value = ''null''::jsonb, settled_at = $1
         from due
        where p.root_id = due.root_id and p.id = due.id
       returning p.root_id, p.execution_id
     ) select root_id, array_agg(distinct execution_id) as owners
         from fired group by root_id order by root_id',
    v_p, v_p
  ) using v_now loop
    perform otra._wake_local(v_queue_id, v_woken.root_id, v_woken.owners);
  end loop;

  for v_woken in execute format(
    'with due as (
       select root_id, id from otra.%I
        where kind in (''event'', ''external'') and status = ''pending''
          and wake_at is not null and wake_at <= $1
        order by wake_at, id limit 100 for update skip locked
     ), timed_out as (
       update otra.%I p
          set status = ''rejected'',
              error = case when p.kind = ''event'' then
                jsonb_build_object(''name'', ''EventTimeoutError'',
                  ''message'', ''timed out waiting for event '' || p.event_name)
              else jsonb_build_object(''name'', ''TimeoutError'',
                  ''message'', ''timed out waiting for external promise "'' || p.label || ''"'') end,
              settled_at = $1
         from due
        where p.root_id = due.root_id and p.id = due.id
       returning p.root_id, p.execution_id
     ) select root_id, array_agg(distinct execution_id) as owners
         from timed_out group by root_id order by root_id',
    v_p, v_p
  ) using v_now loop
    perform otra._wake_local(v_queue_id, v_woken.root_id, v_woken.owners);
  end loop;

  -- Candidates are read WITHOUT locks: _fail_attempt_local acquires its
  -- ordered (self + parent) locks itself and re-checks expiry under them.
  -- Pre-locking children here would invert the global lock order against
  -- the walkers and terminal transitions.
  for v_crashed in execute format(
    'select root_id, id from otra.%I
      where status = ''running'' and claim_expires_at <= $1
      order by claim_expires_at, id limit 100',
    v_x
  ) using v_now loop
    perform * from otra._fail_attempt_local(
      v_queue_id, v_crashed.root_id, v_crashed.id,
      jsonb_build_object('name', 'ClaimExpiredError',
                         'message', 'worker claim expired before completion'),
      true,
      p_only_if_expired => true
    );
  end loop;

  return query execute format(
    'update otra.%1$I e
        set status = ''running'',
            claimed_by = $1,
            claim_expires_at = $2 + make_interval(secs => $3),
            updated_at = $2
      where (e.root_id, e.id) in (
        select c.root_id, c.id
          from otra.%1$I c
         where c.status = ''pending'' and c.run_after <= $2
         order by c.run_after, c.id
         limit $4
           for update skip locked
      )
      returning $5::uuid, e.root_id, e.id, e.function_name, e.params,
                e.attempt, e.max_attempts,
                (e.cancel_requested_at is not null)',
    v_x
  ) using p_worker, v_now, p_claim_seconds, p_batch, v_queue_id;
end;
$$ language plpgsql;

create or replace function otra.load_history_local (
  p_queue uuid,
  p_root uuid,
  p_execution uuid
) returns table (
  id uuid,
  key text,
  label text,
  kind text,
  status text,
  value jsonb,
  error jsonb,
  child_execution_id uuid
) as $$
declare
  v_p text;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then
    raise exception 'Queue % does not exist', p_queue;
  end if;
  v_p := 'p_' || replace(p_queue::text, '-', '');
  return query execute format(
    'select p.id, p.key, p.label, p.kind, p.status, p.value, p.error,
            p.child_execution_id
       from otra.%I p
      where p.root_id = $1 and p.execution_id = $2
      order by p.created_at, p.id',
    v_p
  ) using p_root, p_execution;
end;
$$ language plpgsql;

create or replace function otra.complete_local (
  p_queue uuid,
  p_root uuid,
  p_execution uuid,
  p_worker text,
  p_result jsonb
) returns void as $$
declare
  v_storage text;
  v_x text;
  v_p text;
  v_cancelled boolean;
  v_updated int;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then
    raise exception 'Queue % does not exist', p_queue;
  end if;
  v_storage := replace(p_queue::text, '-', '');
  v_x := 'x_' || v_storage;
  v_p := 'p_' || v_storage;
  -- Global lock order: self + parent, ascending by id, before any write.
  perform otra._lock_terminal_scope(v_x, p_root, p_execution);
  execute format(
    'select exists (
       select 1 from otra.%I
        where root_id = $1 and execution_id = $2 and key = ''$cancel''
     )',
    v_p
  ) into v_cancelled using p_root, p_execution;
  if v_cancelled then
    raise exception 'execution % has a delivered cancellation; it can only finalize as cancelled',
      p_execution;
  end if;
  execute format(
    'update otra.%I
        set status = ''completed'', result = $1, claim_expires_at = null,
            finished_at = otra.now(), updated_at = otra.now()
      where root_id = $2 and id = $3
        and claimed_by = $4 and status = ''running''',
    v_x
  ) using p_result, p_root, p_execution, p_worker;
  get diagnostics v_updated = row_count;
  if v_updated = 0 then
    raise exception 'execution % is not running under worker %', p_execution, p_worker;
  end if;
  perform otra._settle_child_promises_local(
    p_queue, p_root, p_execution, true, p_result, null
  );
end;
$$ language plpgsql;

create or replace function otra.get_execution_local (
  p_queue uuid,
  p_root uuid,
  p_execution uuid
) returns table (
  id uuid, queue text, function_name text, status text, attempt int,
  params jsonb, result jsonb, error jsonb,
  parent_id uuid, root_id uuid,
  cancel_requested_at timestamptz, cancel_reason text,
  created_at timestamptz, finished_at timestamptz
) as $$
declare
  v_queue_name text;
  v_x text;
begin
  select q.name into v_queue_name
    from otra.queues q where q.id = p_queue for key share;
  if not found then
    raise exception 'Queue % does not exist', p_queue;
  end if;
  v_x := 'x_' || replace(p_queue::text, '-', '');
  return query execute format(
    'select e.id, $1::text, e.function_name, e.status, e.attempt,
            e.params, e.result, e.error, e.parent_id, e.root_id,
            e.cancel_requested_at, e.cancel_reason, e.created_at, e.finished_at
       from otra.%I e
      where e.root_id = $2 and e.id = $3',
    v_x
  ) using v_queue_name, p_root, p_execution;
end;
$$ language plpgsql;

create or replace function otra._assert_owner_local (
  p_queue uuid,
  p_root uuid,
  p_execution uuid,
  p_worker text
) returns void as $$
declare
  v_x text := 'x_' || replace(p_queue::text, '-', '');
  v_status text;
  v_claimed_by text;
begin
  execute format(
    'select status, claimed_by from otra.%I
      where root_id = $1 and id = $2 for update',
    v_x
  ) into v_status, v_claimed_by using p_root, p_execution;
  if v_status = 'cancelled' then
    raise exception 'execution % was cancelled', p_execution
      using errcode = 'OT002';
  end if;
  if v_status is null or v_status <> 'running'
     or v_claimed_by is distinct from p_worker then
    raise exception 'worker % no longer holds the claim on execution %',
      p_worker, p_execution using errcode = 'OT001';
  end if;
end;
$$ language plpgsql;

-- Lock the executions every terminal transition of p_execution touches: the
-- row itself plus its parent (whose child-promise rows get settled and whose
-- execution row _wake_local then locks).  Acquired in ascending id order --
-- the one global lock order shared with the cancel/kill tree walks,
-- _wake_local, and cleanup.  NOTE: uuid_v7 ids created in the same
-- millisecond are NOT guaranteed parent-before-child, which is exactly why
-- this orders by id instead of by tree position.  Rows may be gone (cleaned
-- up); callers re-check what they need with the conditional writes they
-- already do.
create or replace function otra._lock_terminal_scope (
  p_x text, p_root uuid, p_execution uuid
) returns void as $$
declare
  v_parent uuid;
begin
  execute format(
    'select parent_id from otra.%I where root_id = $1 and id = $2',
    p_x
  ) into v_parent using p_root, p_execution;
  execute format(
    'select 1 from otra.%I
      where root_id = $1 and id = any ($2)
      order by id for update',
    p_x
  ) using p_root, array_remove(array[p_execution, v_parent], null);
end;
$$ language plpgsql;

create or replace function otra._wake_local (
  p_queue uuid,
  p_root uuid,
  p_execution_ids uuid[]
) returns void as $$
declare
  v_x text := 'x_' || replace(p_queue::text, '-', '');
  v_woken int;
  v_name text;
begin
  if p_execution_ids is null or array_length(p_execution_ids, 1) is null then
    return;
  end if;
  execute format(
    'select 1 from otra.%I
      where root_id = $1 and id = any ($2)
      order by id for update',
    v_x
  ) using p_root, p_execution_ids;
  execute format(
    'update otra.%I
        set status = ''pending'', run_after = otra.now(), updated_at = otra.now()
      where root_id = $1 and id = any ($2) and status = ''suspended''',
    v_x
  ) using p_root, p_execution_ids;
  get diagnostics v_woken = row_count;
  if v_woken > 0 then
    select q.name into v_name from otra.queues q where q.id = p_queue;
    perform pg_notify('otra_wake', v_name);
  end if;
end;
$$ language plpgsql;

create or replace function otra._settle_child_promises_local (
  p_queue uuid,
  p_root uuid,
  p_child uuid,
  p_resolved boolean,
  p_value jsonb,
  p_error jsonb
) returns void as $$
declare
  v_p text := 'p_' || replace(p_queue::text, '-', '');
  v_owners uuid[];
begin
  execute format(
    'with settled as (
       update otra.%I
          set status = case when $3 then ''resolved'' else ''rejected'' end,
              value = case when $3 then $4 else null end,
              error = case when $3 then null else $5 end,
              settled_at = otra.now()
        where root_id = $1 and child_execution_id = $2
          and kind = ''child'' and status = ''pending''
       returning execution_id
     ) select array_agg(distinct execution_id) from settled',
    v_p
  ) into v_owners using p_root, p_child, p_resolved, p_value, p_error;
  perform otra._wake_local(p_queue, p_root, v_owners);
end;
$$ language plpgsql;

create or replace function otra.spawn_child_local (
  p_queue uuid,
  p_root uuid,
  p_parent uuid,
  p_worker text,
  p_key text,
  p_label text,
  p_function text,
  p_params jsonb,
  p_opts jsonb default '{}'
) returns table (
  queue_id uuid,
  root_id uuid,
  execution_id uuid,
  created boolean
) as $$
declare
  v_storage text;
  v_x text;
  v_p text;
  v_existing uuid;
  v_id uuid := otra.uuid_v7();
  v_delay double precision := coalesce((p_opts ->> 'delay_s')::double precision, 0);
  v_strategy jsonb := coalesce(
    p_opts -> 'retry_strategy',
    '{"kind": "exponential", "base_s": 1, "factor": 2, "max_s": 300}'::jsonb
  );
  v_name text;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  perform otra._backoff(v_strategy, 1);
  perform otra._assert_owner_local(p_queue, p_root, p_parent, p_worker);
  v_storage := replace(p_queue::text, '-', '');
  v_x := 'x_' || v_storage;
  v_p := 'p_' || v_storage;
  execute format(
    'select child_execution_id from otra.%I
      where root_id = $1 and execution_id = $2 and key = $3',
    v_p
  ) into v_existing using p_root, p_parent, p_key;
  if v_existing is not null then
    return query select p_queue, p_root, v_existing, false;
    return;
  end if;
  execute format(
    'insert into otra.%I
       (id, root_id, function_name, params, parent_id, max_attempts,
        retry_strategy, run_after, on_parent_cancel)
     values ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
    v_x
  ) using
    v_id, p_root, p_function, p_params, p_parent,
    coalesce((p_opts ->> 'max_attempts')::int, 5), v_strategy,
    otra.now() + make_interval(secs => v_delay),
    coalesce(p_opts ->> 'on_parent_cancel', 'cascade');
  execute format(
    'insert into otra.%I
       (root_id, execution_id, key, label, kind, child_execution_id)
     values ($1, $2, $3, $4, ''child'', $5)',
    v_p
  ) using p_root, p_parent, p_key, p_label, v_id;
  select q.name into v_name from otra.queues q where q.id = p_queue;
  perform pg_notify('otra_wake', v_name);
  return query select p_queue, p_root, v_id, true;
end;
$$ language plpgsql;

create or replace function otra.record_run_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_worker text,
  p_key text, p_label text, p_value jsonb,
  p_claim_seconds double precision default 30
) returns jsonb as $$
declare
  v_storage text := replace(p_queue::text, '-', '');
  v_x text := 'x_' || v_storage;
  v_p text := 'p_' || v_storage;
  v_value jsonb;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  perform otra._assert_owner_local(p_queue, p_root, p_execution, p_worker);
  execute format(
    'insert into otra.%I
       (root_id, execution_id, key, label, kind, status, value, settled_at)
     values ($1, $2, $3, $4, ''run'', ''resolved'', $5, otra.now())
     on conflict (root_id, execution_id, key) do nothing',
    v_p
  ) using p_root, p_execution, p_key, p_label, p_value;
  execute format(
    'select value from otra.%I
      where root_id = $1 and execution_id = $2 and key = $3',
    v_p
  ) into v_value using p_root, p_execution, p_key;
  execute format(
    'update otra.%I
        set claim_expires_at = otra.now() + make_interval(secs => $1),
            updated_at = otra.now()
      where root_id = $2 and id = $3
        and claimed_by = $4 and status = ''running''',
    v_x
  ) using p_claim_seconds, p_root, p_execution, p_worker;
  return v_value;
end;
$$ language plpgsql;

create or replace function otra.create_sleep_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_worker text,
  p_key text, p_label text, p_seconds double precision
) returns table (status text, value jsonb, error jsonb) as $$
declare v_p text := 'p_' || replace(p_queue::text, '-', '');
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  perform otra._assert_owner_local(p_queue, p_root, p_execution, p_worker);
  execute format(
    'insert into otra.%I
       (root_id, execution_id, key, label, kind, wake_at)
     values ($1, $2, $3, $4, ''sleep'', otra.now() + make_interval(secs => $5))
     on conflict (root_id, execution_id, key) do nothing',
    v_p
  ) using p_root, p_execution, p_key, p_label, p_seconds;
  return query execute format(
    'select p.status, p.value, p.error from otra.%I p
      where p.root_id = $1 and p.execution_id = $2 and p.key = $3',
    v_p
  ) using p_root, p_execution, p_key;
end;
$$ language plpgsql;

create or replace function otra.create_external_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_worker text,
  p_key text, p_label text, p_timeout_seconds double precision default null
) returns table (id uuid, status text, value jsonb, error jsonb) as $$
declare v_p text := 'p_' || replace(p_queue::text, '-', '');
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  perform otra._assert_owner_local(p_queue, p_root, p_execution, p_worker);
  execute format(
    'insert into otra.%I
       (root_id, execution_id, key, label, kind, wake_at)
     values ($1, $2, $3, $4, ''external'',
       case when $5 is null then null
            else otra.now() + make_interval(secs => $5) end)
     on conflict (root_id, execution_id, key) do nothing',
    v_p
  ) using p_root, p_execution, p_key, p_label, p_timeout_seconds;
  return query execute format(
    'select p.id, p.status, p.value, p.error from otra.%I p
      where p.root_id = $1 and p.execution_id = $2 and p.key = $3',
    v_p
  ) using p_root, p_execution, p_key;
end;
$$ language plpgsql;

create or replace function otra.create_event_wait_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_worker text,
  p_key text, p_label text, p_event_name text,
  p_timeout_seconds double precision default null
) returns table (status text, value jsonb, error jsonb) as $$
declare
  v_storage text := replace(p_queue::text, '-', '');
  v_p text := 'p_' || v_storage;
  v_e text := 'e_' || v_storage;
  v_payload jsonb;
  v_count int;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  perform pg_advisory_xact_lock(
    hashtextextended('otra:event:' || p_queue::text || ':' || p_event_name, 0)
  );
  perform otra._assert_owner_local(p_queue, p_root, p_execution, p_worker);
  execute format(
    'select payload from otra.%I where name = $1',
    v_e
  ) into v_payload using p_event_name;
  get diagnostics v_count = row_count;
  if v_count > 0 then
    execute format(
      'insert into otra.%I
         (root_id, execution_id, key, label, kind, status, value,
          event_name, settled_at)
       values ($1, $2, $3, $4, ''event'', ''resolved'', $5, $6, otra.now())
       on conflict (root_id, execution_id, key) do nothing',
      v_p
    ) using p_root, p_execution, p_key, p_label, v_payload, p_event_name;
  else
    execute format(
      'insert into otra.%I
         (root_id, execution_id, key, label, kind, event_name, wake_at)
       values ($1, $2, $3, $4, ''event'', $5,
         case when $6 is null then null
              else otra.now() + make_interval(secs => $6) end)
       on conflict (root_id, execution_id, key) do nothing',
      v_p
    ) using p_root, p_execution, p_key, p_label, p_event_name, p_timeout_seconds;
  end if;
  return query execute format(
    'select p.status, p.value, p.error from otra.%I p
      where p.root_id = $1 and p.execution_id = $2 and p.key = $3',
    v_p
  ) using p_root, p_execution, p_key;
end;
$$ language plpgsql;

create or replace function otra.emit_event_local (
  p_queue text, p_name text, p_payload jsonb default null
) returns void as $$
declare
  v_queue_id uuid;
  v_storage text;
  v_e text;
  v_p text;
  v_event_id uuid;
  v_woken record;
begin
  select q.id into v_queue_id from otra.queues q
   where q.name = p_queue for key share;
  if not found then raise exception 'Queue "%" does not exist', p_queue; end if;
  v_storage := replace(v_queue_id::text, '-', '');
  v_e := 'e_' || v_storage;
  v_p := 'p_' || v_storage;
  perform pg_advisory_xact_lock(
    hashtextextended('otra:event:' || v_queue_id::text || ':' || p_name, 0)
  );
  execute format(
    'insert into otra.%I (name, payload) values ($1, $2)
     on conflict (name) do nothing returning id',
    v_e
  ) into v_event_id using p_name, p_payload;
  if v_event_id is null then return; end if;
  for v_woken in execute format(
    'with settled as (
       update otra.%I
          set status = ''resolved'', value = $1, settled_at = otra.now()
        where kind = ''event'' and status = ''pending'' and event_name = $2
       returning root_id, execution_id
     ) select root_id, array_agg(distinct execution_id) as owners
         from settled group by root_id order by root_id',
    v_p
  ) using p_payload, p_name loop
    perform otra._wake_local(v_queue_id, v_woken.root_id, v_woken.owners);
  end loop;
end;
$$ language plpgsql;

create or replace function otra.get_promises_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_keys text[]
) returns table (
  id uuid, key text, kind text, status text, value jsonb, error jsonb,
  child_execution_id uuid
) as $$
declare v_p text := 'p_' || replace(p_queue::text, '-', '');
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  return query execute format(
    'select p.id, p.key, p.kind, p.status, p.value, p.error,
            p.child_execution_id
       from otra.%I p
      where p.root_id = $1 and p.execution_id = $2 and p.key = any ($3)',
    v_p
  ) using p_root, p_execution, p_keys;
end;
$$ language plpgsql;

drop function if exists otra.suspend_local (uuid, uuid, uuid, text, text[]);
create or replace function otra.suspend_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_worker text,
  p_blocker_keys text[],
  p_shielded boolean default false
) returns table (suspended boolean, cancel_requested boolean) as $$
declare
  v_storage text := replace(p_queue::text, '-', '');
  v_x text := 'x_' || v_storage;
  v_p text := 'p_' || v_storage;
  v_status text;
  v_claimed_by text;
  v_cancel timestamptz;
  v_exists boolean;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  execute format(
    'select status, claimed_by, cancel_requested_at from otra.%I
      where root_id = $1 and id = $2 for update',
    v_x
  ) into v_status, v_claimed_by, v_cancel using p_root, p_execution;
  if v_status is null or v_status <> 'running'
     or v_claimed_by is distinct from p_worker then
    return query select false, false; return;
  end if;
  execute format(
    'select exists (select 1 from otra.%I
      where root_id = $1 and execution_id = $2 and key = ''$cancel'')',
    v_p
  ) into v_exists using p_root, p_execution;
  -- A pending cancel blocks parking only BEFORE delivery -- the driver must
  -- deliver instead -- UNLESS the generator is inside ctx.uninterruptible:
  -- a shielded critical section may suspend, and delivery waits until the
  -- shield exits (otherwise the section would commit in half).
  if v_cancel is not null and not v_exists and not p_shielded then
    return query select false, true; return;
  end if;
  execute format(
    'select exists (select 1 from otra.%I
      where root_id = $1 and execution_id = $2
        and key = any ($3) and status <> ''pending'')',
    v_p
  ) into v_exists using p_root, p_execution, p_blocker_keys;
  if v_exists then return query select false, false; return; end if;
  execute format(
    'update otra.%I
        set status = ''suspended'', claimed_by = null,
            claim_expires_at = null, updated_at = otra.now()
      where root_id = $1 and id = $2',
    v_x
  ) using p_root, p_execution;
  return query select true, false;
end;
$$ language plpgsql;

create or replace function otra.resolve_promise_local (
  p_queue uuid, p_root uuid, p_id uuid, p_value jsonb
) returns boolean as $$
declare
  v_p text := 'p_' || replace(p_queue::text, '-', '');
  v_owner uuid;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  execute format(
    'update otra.%I
        set status = ''resolved'', value = $1, settled_at = otra.now()
      where root_id = $2 and id = $3
        and kind = ''external'' and status = ''pending''
      returning execution_id',
    v_p
  ) into v_owner using p_value, p_root, p_id;
  if v_owner is null then return false; end if;
  perform otra._wake_local(p_queue, p_root, array[v_owner]);
  return true;
end;
$$ language plpgsql;

create or replace function otra.reject_promise_local (
  p_queue uuid, p_root uuid, p_id uuid, p_error jsonb
) returns boolean as $$
declare
  v_p text := 'p_' || replace(p_queue::text, '-', '');
  v_owner uuid;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  execute format(
    'update otra.%I
        set status = ''rejected'', error = $1, settled_at = otra.now()
      where root_id = $2 and id = $3
        and kind = ''external'' and status = ''pending''
      returning execution_id',
    v_p
  ) into v_owner using p_error, p_root, p_id;
  if v_owner is null then return false; end if;
  perform otra._wake_local(p_queue, p_root, array[v_owner]);
  return true;
end;
$$ language plpgsql;

create or replace function otra.record_cancel_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_worker text, p_position jsonb
) returns jsonb as $$
declare
  v_p text := 'p_' || replace(p_queue::text, '-', '');
  v_position jsonb;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  perform otra._assert_owner_local(p_queue, p_root, p_execution, p_worker);
  execute format(
    'insert into otra.%I
       (root_id, execution_id, key, label, kind, status, value, settled_at)
     values ($1, $2, ''$cancel'', ''$cancel'', ''cancel'', ''resolved'', $3, otra.now())
     on conflict (root_id, execution_id, key) do nothing',
    v_p
  ) using p_root, p_execution, p_position;
  execute format(
    'select value from otra.%I
      where root_id = $1 and execution_id = $2 and key = ''$cancel''',
    v_p
  ) into v_position using p_root, p_execution;
  return v_position;
end;
$$ language plpgsql;

drop function if exists otra._fail_attempt_local (uuid, uuid, uuid, jsonb, boolean);
create or replace function otra._fail_attempt_local (
  p_queue uuid, p_root uuid, p_execution uuid,
  p_error jsonb, p_retryable boolean,
  p_worker text default null,
  p_only_if_expired boolean default false
) returns table (applied boolean, failed_permanently boolean, retry_at timestamptz) as $$
declare
  v_storage text := replace(p_queue::text, '-', '');
  v_x text := 'x_' || v_storage;
  v_p text := 'p_' || v_storage;
  v_row record;
  v_delivered boolean;
  v_retry_at timestamptz;
begin
  -- Global lock order (self + parent ascending) BEFORE reading state, so a
  -- failure transition can never hold the child while waiting on a walker
  -- that holds the parent.
  perform otra._lock_terminal_scope(v_x, p_root, p_execution);
  execute format(
    'select status, attempt, max_attempts, retry_strategy, cancel_requested_at,
            claimed_by, claim_expires_at
       from otra.%I where root_id = $1 and id = $2',
    v_x
  ) into v_row using p_root, p_execution;
  if v_row.status is null or v_row.status in ('completed', 'failed', 'cancelled') then
    return query select false, false, null::timestamptz; return;
  end if;
  -- Ownership guard (when a worker reports the failure): a zombie whose
  -- lease was stolen must not knock a live worker's execution back.
  if p_worker is not null
     and (v_row.status <> 'running' or v_row.claimed_by is distinct from p_worker) then
    return query select false, false, null::timestamptz; return;
  end if;
  -- Expiry guard (when the claim sweep reports it): the candidate was read
  -- without a lock, so re-check under it -- another sweep may have won.
  if p_only_if_expired
     and (v_row.status <> 'running'
          or v_row.claim_expires_at is null
          or v_row.claim_expires_at > otra.now()) then
    return query select false, false, null::timestamptz; return;
  end if;
  if p_retryable and v_row.attempt + 1 < v_row.max_attempts then
    execute format(
      'select exists (select 1 from otra.%I
        where root_id = $1 and execution_id = $2 and key = ''$cancel'')',
      v_p
    ) into v_delivered using p_root, p_execution;
    if v_row.cancel_requested_at is not null and not v_delivered then
      -- A cancel is pending but CancelledError has not been delivered yet:
      -- the retry exists only so a worker can deliver it, so it must run
      -- now, not after the backoff.  (Once '$cancel' is journaled, failures
      -- are compensation retries and keep their backoff -- a failing
      -- compensation step must not hot-loop.)
      v_retry_at := otra.now();
    else
      begin
        v_retry_at := otra.now() + otra._backoff(v_row.retry_strategy, v_row.attempt + 1);
      exception when others then v_retry_at := null;
      end;
    end if;
  end if;
  if v_retry_at is not null then
    execute format(
      'update otra.%I
          set status = ''pending'', attempt = attempt + 1, run_after = $1,
              claimed_by = null, claim_expires_at = null, error = $2,
              updated_at = otra.now()
        where root_id = $3 and id = $4',
      v_x
    ) using v_retry_at, p_error, p_root, p_execution;
    return query select true, false, v_retry_at;
  else
    execute format(
      'update otra.%I
          set status = case when cancel_requested_at is not null
                            then ''cancelled'' else ''failed'' end,
              attempt = attempt + 1, claim_expires_at = null, error = $1,
              finished_at = otra.now(), updated_at = otra.now()
        where root_id = $2 and id = $3',
      v_x
    ) using p_error, p_root, p_execution;
    perform otra._settle_child_promises_local(
      p_queue, p_root, p_execution, false, null,
      case when v_row.cancel_requested_at is not null then
        jsonb_build_object('name', 'CancelledError', 'message', 'execution was cancelled')
      else p_error end
    );
    return query select true, true, null::timestamptz;
  end if;
end;
$$ language plpgsql;

create or replace function otra.fail_attempt_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_worker text,
  p_error jsonb, p_retryable boolean default true
) returns table (
  applied boolean, failed_permanently boolean, retry_at timestamptz
) as $$
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  -- Ownership is verified inside _fail_attempt_local, under its ordered
  -- locks (a separate assert-then-lock would acquire the child first and
  -- break the global lock order).
  return query select * from otra._fail_attempt_local(
    p_queue, p_root, p_execution, p_error, p_retryable, p_worker
  );
end;
$$ language plpgsql;

create or replace function otra.extend_claim_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_worker text,
  p_claim_seconds double precision default 30
) returns table (held boolean, cancel_requested boolean) as $$
declare
  v_x text := 'x_' || replace(p_queue::text, '-', '');
  v_cancel timestamptz;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  execute format(
    'update otra.%I
        set claim_expires_at = otra.now() + make_interval(secs => $1),
            updated_at = otra.now()
      where root_id = $2 and id = $3
        and claimed_by = $4 and status = ''running''
      returning cancel_requested_at',
    v_x
  ) into v_cancel using p_claim_seconds, p_root, p_execution, p_worker;
  if v_cancel is null then
    -- Distinguish a held row without cancellation from no matching row.
    execute format(
      'select exists (select 1 from otra.%I
        where root_id = $1 and id = $2
          and claimed_by = $3 and status = ''running'')',
      v_x
    ) into held using p_root, p_execution, p_worker;
    return query select held, false;
  else
    return query select true, true;
  end if;
end;
$$ language plpgsql;

create or replace function otra.defer_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_worker text,
  p_delay_seconds double precision default 15
) returns boolean as $$
declare
  v_x text := 'x_' || replace(p_queue::text, '-', '');
  v_updated int;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  execute format(
    'update otra.%I
        set status = ''pending'',
            run_after = otra.now() + make_interval(secs => $1),
            claimed_by = null, claim_expires_at = null, updated_at = otra.now()
      where root_id = $2 and id = $3
        and claimed_by = $4 and status = ''running''',
    v_x
  ) using p_delay_seconds, p_root, p_execution, p_worker;
  get diagnostics v_updated = row_count;
  return v_updated > 0;
end;
$$ language plpgsql;

create or replace function otra.request_cancel_local (
  p_queue uuid, p_root uuid, p_execution uuid,
  p_cascade boolean default true, p_reason text default null
) returns table (execution_id uuid, action text) as $$
declare
  v_storage text := replace(p_queue::text, '-', '');
  v_x text := 'x_' || v_storage;
  v_p text := 'p_' || v_storage;
  v_now timestamptz := otra.now();
  v_row record;
  v_tree uuid[];
  v_parent uuid;
  v_has_history boolean;
  v_name text;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  -- Snapshot the tree membership unlocked, then take ONE lock statement in
  -- the global ascending-id order: the target's parent (finalizing the
  -- target settles promise rows there and wakes it) plus every non-terminal
  -- member.  Terminal members are never locked -- they only produce 'noop'
  -- rows -- which keeps the walk out of cleanup_local's way entirely.
  execute format(
    'with recursive tree as (
       select id from otra.%1$I where root_id = $1 and id = $2
       union all
       select c.id from otra.%1$I c join tree t on c.parent_id = t.id
        where c.root_id = $1 and $3 and c.on_parent_cancel = ''cascade''
     ) select array_agg(id) from tree',
    v_x
  ) into v_tree using p_root, p_execution, p_cascade;
  if v_tree is null then return; end if;
  execute format(
    'select parent_id from otra.%I where root_id = $1 and id = $2',
    v_x
  ) into v_parent using p_root, p_execution;
  execute format(
    'select 1 from otra.%I
      where root_id = $1
        and (id = $3 or (id = any ($2)
             and status not in (''completed'', ''failed'', ''cancelled'')))
      order by id for update',
    v_x
  ) using p_root, v_tree, v_parent;
  for v_row in execute format(
    'select e.id, e.status from otra.%I e
      where e.root_id = $1 and e.id = any ($2)
      order by e.id',
    v_x
  ) using p_root, v_tree loop
    if v_row.status in ('completed', 'failed', 'cancelled') then
      execution_id := v_row.id; action := 'noop'; return next; continue;
    end if;
    execute format(
      'update otra.%I
          set cancel_requested_at = coalesce(cancel_requested_at, $1),
              cancel_reason = coalesce(cancel_reason, $2), updated_at = $1
        where root_id = $3 and id = $4',
      v_x
    ) using v_now, p_reason, p_root, v_row.id;
    execute format(
      'select exists (select 1 from otra.%I
        where root_id = $1 and execution_id = $2)',
      v_p
    ) into v_has_history using p_root, v_row.id;
    if v_row.status = 'pending' and not v_has_history then
      execute format(
        'update otra.%I
            set status = ''cancelled'',
                error = jsonb_build_object(''name'', ''CancelledError'',
                  ''message'', coalesce($1, ''execution was cancelled'')),
                finished_at = $2, updated_at = $2
          where root_id = $3 and id = $4',
        v_x
      ) using p_reason, v_now, p_root, v_row.id;
      perform otra._settle_child_promises_local(
        p_queue, p_root, v_row.id, false, null,
        jsonb_build_object('name', 'CancelledError', 'message', 'execution was cancelled')
      );
      execution_id := v_row.id; action := 'cancelled'; return next;
    elsif v_row.status = 'suspended' then
      execute format(
        'update otra.%I set status = ''pending'', run_after = $1, updated_at = $1
          where root_id = $2 and id = $3',
        v_x
      ) using v_now, p_root, v_row.id;
      execution_id := v_row.id; action := 'woken'; return next;
    elsif v_row.status = 'pending' then
      -- Waiting out a retry backoff: expedite it, or the cancel would sit
      -- undelivered until the retry was due (up to the backoff cap).
      execute format(
        'update otra.%I set run_after = least(run_after, $1), updated_at = $1
          where root_id = $2 and id = $3',
        v_x
      ) using v_now, p_root, v_row.id;
      execution_id := v_row.id; action := 'requested'; return next;
    else
      execution_id := v_row.id; action := 'requested'; return next;
    end if;
  end loop;
  select q.name into v_name from otra.queues q where q.id = p_queue;
  perform pg_notify('otra_wake', v_name);
end;
$$ language plpgsql;

create or replace function otra.kill_local (
  p_queue uuid, p_root uuid, p_execution uuid,
  p_cascade boolean default true, p_reason text default null
) returns int as $$
declare
  v_x text := 'x_' || replace(p_queue::text, '-', '');
  v_now timestamptz := otra.now();
  v_row record;
  v_tree uuid[];
  v_parent uuid;
  v_count int := 0;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  -- Same lock discipline as request_cancel_local: snapshot membership, then
  -- one ascending-id lock statement over the target's parent plus the
  -- non-terminal members.
  execute format(
    'with recursive tree as (
       select id from otra.%1$I where root_id = $1 and id = $2
       union all
       select c.id from otra.%1$I c join tree t on c.parent_id = t.id
        where c.root_id = $1 and $3 and c.on_parent_cancel = ''cascade''
     ) select array_agg(id) from tree',
    v_x
  ) into v_tree using p_root, p_execution, p_cascade;
  if v_tree is null then return 0; end if;
  execute format(
    'select parent_id from otra.%I where root_id = $1 and id = $2',
    v_x
  ) into v_parent using p_root, p_execution;
  execute format(
    'select 1 from otra.%I
      where root_id = $1
        and (id = $3 or (id = any ($2)
             and status not in (''completed'', ''failed'', ''cancelled'')))
      order by id for update',
    v_x
  ) using p_root, v_tree, v_parent;
  for v_row in execute format(
    'select e.id, e.status from otra.%I e
      where e.root_id = $1 and e.id = any ($2)
      order by e.id',
    v_x
  ) using p_root, v_tree loop
    if v_row.status in ('completed', 'failed', 'cancelled') then continue; end if;
    execute format(
      'update otra.%I
          set status = ''cancelled'',
              cancel_requested_at = coalesce(cancel_requested_at, $1),
              cancel_reason = coalesce(cancel_reason, $2),
              claim_expires_at = null,
              error = jsonb_build_object(''name'', ''CancelledError'',
                ''message'', coalesce($2, ''execution was killed'')),
              finished_at = $1, updated_at = $1
        where root_id = $3 and id = $4',
      v_x
    ) using v_now, p_reason, p_root, v_row.id;
    perform otra._settle_child_promises_local(
      p_queue, p_root, v_row.id, false, null,
      jsonb_build_object('name', 'CancelledError', 'message', 'execution was cancelled')
    );
    v_count := v_count + 1;
  end loop;
  return v_count;
end;
$$ language plpgsql;

create or replace function otra.finalize_cancelled_local (
  p_queue uuid, p_root uuid, p_execution uuid, p_worker text,
  p_error jsonb default null
) returns boolean as $$
declare
  v_x text := 'x_' || replace(p_queue::text, '-', '');
  v_updated int;
begin
  perform 1 from otra.queues q where q.id = p_queue for key share;
  if not found then raise exception 'Queue % does not exist', p_queue; end if;
  perform otra._lock_terminal_scope(v_x, p_root, p_execution);
  execute format(
    'update otra.%I
        set status = ''cancelled'', claim_expires_at = null,
            error = coalesce($1, error), finished_at = otra.now(),
            updated_at = otra.now()
      where root_id = $2 and id = $3 and claimed_by = $4
        and status = ''running'' and cancel_requested_at is not null',
    v_x
  ) using p_error, p_root, p_execution, p_worker;
  get diagnostics v_updated = row_count;
  if v_updated = 0 then return false; end if;
  perform otra._settle_child_promises_local(
    p_queue, p_root, p_execution, false, null,
    jsonb_build_object('name', 'CancelledError', 'message', 'execution was cancelled')
  );
  return true;
end;
$$ language plpgsql;

create or replace function otra.cleanup_local (
  p_queue text,
  p_ttl interval default null,
  p_limit int default null
) returns void as $$
declare
  v_queue_id uuid;
  v_mode text;
  v_ttl interval;
  v_limit int;
  v_storage text;
  v_x text;
  v_p text;
  v_e text;
  v_i text;
  v_roots uuid[];
begin
  select q.id, q.storage_mode, coalesce(p_ttl, q.cleanup_ttl),
         coalesce(p_limit, q.cleanup_limit)
    into v_queue_id, v_mode, v_ttl, v_limit
    from otra.queues q where q.name = p_queue for key share;
  if not found then raise exception 'Queue "%" does not exist', p_queue; end if;
  v_storage := replace(v_queue_id::text, '-', '');
  v_x := 'x_' || v_storage;
  v_p := 'p_' || v_storage;
  v_e := 'e_' || v_storage;
  v_i := 'i_' || v_storage;
  execute format(
    'select array_agg(id) from (
       select r.id from otra.%1$I r
        where r.root_id = r.id
          and r.status in (''completed'', ''failed'', ''cancelled'')
          and r.finished_at < otra.now() - $1
          and not exists (
            select 1 from otra.%1$I d
             where d.root_id = r.id
               and d.status not in (''completed'', ''failed'', ''cancelled'')
          )
        order by r.finished_at, r.id limit $2
     ) candidates',
    v_x
  ) into v_roots using v_ttl, v_limit;
  if v_roots is not null then
    -- Take every row of the candidate trees in the global ascending order
    -- BEFORE deleting: the deletes below then only re-take held locks, so
    -- cleanup cannot ABBA against walkers or terminal transitions.  (All
    -- members are terminal -- walkers don't lock terminal rows -- so this
    -- only ever contends with another cleanup.)
    execute format(
      'select 1 from otra.%I where root_id = any ($1)
        order by root_id, id for update',
      v_x
    ) using v_roots;
    if v_mode = 'partitioned' then
      execute format('delete from otra.%I where root_id = any ($1)', v_i)
        using v_roots;
    end if;
    -- Promise rows first: resolvers lock promise -> execution, and deleting
    -- the execution rows first would cascade into promise rows in the
    -- opposite order.
    execute format('delete from otra.%I where root_id = any ($1)', v_p)
      using v_roots;
    execute format('delete from otra.%I where root_id = any ($1)', v_x)
      using v_roots;
  end if;
  execute format(
    'delete from otra.%1$I where id in (
       select id from otra.%1$I
        where created_at < otra.now() - $1
        order by created_at, id limit $2
     )',
    v_e
  ) using v_ttl, v_limit;
end;
$$ language plpgsql;
