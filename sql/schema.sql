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

create table if not exists otra.executions (
  id               uuid primary key default otra.uuid_v7(),
  queue            text not null default 'default',
  function_name    text not null,
  params           jsonb,
  -- pending    -> runnable, waiting to be claimed
  -- running    -> claimed by a worker
  -- suspended  -> parked on unresolved remote promises (zero footprint)
  -- completed / failed / cancelled -> terminal
  status           text not null default 'pending',
  attempt          int not null default 0,          -- failed attempts so far
  max_attempts     int not null default 5,
  retry_strategy   jsonb not null default '{"kind": "exponential", "base_s": 1, "factor": 2, "max_s": 300}',
  run_after        timestamptz not null default otra.now(),
  claimed_by       text,
  claim_expires_at timestamptz,
  parent_id        uuid references otra.executions (id) on delete cascade,
  root_id          uuid,
  result           jsonb,
  error            jsonb,
  idempotency_key  text,
  -- Graceful cancellation is a REQUEST against a live execution, not a
  -- status flip: status stays 'running' through the compensation window so
  -- every ownership-guarded history write keeps working.  See
  -- docs/cancellation-design.md.
  cancel_requested_at timestamptz,
  cancel_reason    text,
  -- What a parent's graceful cancel does to this child.
  on_parent_cancel text not null default 'cascade',
  created_at       timestamptz not null default otra.now(),
  updated_at       timestamptz not null default otra.now(),
  finished_at      timestamptz,
  constraint executions_status_check check (
    status in ('pending', 'running', 'suspended', 'completed', 'failed', 'cancelled')
  ),
  constraint executions_on_parent_cancel_check check (
    on_parent_cancel in ('cascade', 'detach')
  )
);

create index if not exists executions_claimable_idx
  on otra.executions (queue, run_after)
  where status = 'pending';

create index if not exists executions_expiry_idx
  on otra.executions (queue, claim_expires_at)
  where status = 'running';

create index if not exists executions_parent_idx
  on otra.executions (parent_id)
  where parent_id is not null;

create unique index if not exists executions_idempotency_idx
  on otra.executions (queue, idempotency_key)
  where idempotency_key is not null;

create table if not exists otra.promises (
  id                 uuid primary key default otra.uuid_v7(),
  execution_id       uuid not null references otra.executions (id) on delete cascade,
  key                text not null,   -- deterministic key within the execution
  label              text not null,   -- human label, doubles as determinism check
  kind               text not null,   -- run | sleep | event | child
  status             text not null default 'pending',  -- pending | resolved | rejected
  value              jsonb,
  error              jsonb,
  wake_at            timestamptz,     -- sleep: resolve at; event: reject (timeout) at
  event_name         text,
  child_execution_id uuid references otra.executions (id) on delete cascade,
  created_at         timestamptz not null default otra.now(),
  settled_at         timestamptz,
  constraint promises_kind_check check (kind in ('run', 'sleep', 'event', 'child', 'external', 'cancel')),
  constraint promises_status_check check (status in ('pending', 'resolved', 'rejected')),
  constraint promises_key_unique unique (execution_id, key)
);

create index if not exists promises_due_timer_idx
  on otra.promises (wake_at)
  where status = 'pending' and wake_at is not null;

create index if not exists promises_event_wait_idx
  on otra.promises (event_name)
  where status = 'pending' and kind = 'event';

create index if not exists promises_child_idx
  on otra.promises (child_execution_id)
  where kind = 'child';

-- An event is an immutable one-shot FACT per (queue, name): first write
-- wins, later emits are no-ops, and every wait -- including a repeat wait in
-- the same execution -- resolves with that same fact.  absurd converged on
-- exactly these semantics after shipping mutable events (their 7b63b7a);
-- recurring signals derive names ("tick:${i}") or use ctx.promise instead.
create table if not exists otra.events (
  id         uuid primary key default otra.uuid_v7(),
  queue      text not null,
  name       text not null,
  payload    jsonb,
  created_at timestamptz not null default otra.now()
);

create unique index if not exists events_fact_idx
  on otra.events (queue, name);

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
end;
$$ language plpgsql;

create or replace function otra.ensure_partitions (p_name text default null) returns void as $$
declare
  v_now timestamptz := otra.now();
  v_start timestamptz;
  v_end timestamptz;
  v_week timestamptz;
  v_next timestamptz;
  v_lower uuid;
  v_upper uuid;
  v_tag text;
  v_storage text;
  v_x text;
  v_p text;
  v_queue record;
begin
  if p_name is not null and not exists (select 1 from otra.queues where name = p_name) then
    raise exception 'Queue "%" does not exist', p_name;
  end if;

  for v_queue in
    select q.id, q.name, q.default_partition,
           q.partition_lookahead, q.partition_lookback
      from otra.queues q
     where q.storage_mode = 'partitioned'
       and (p_name is null or q.name = p_name)
     order by q.id
       for update
  loop
    v_storage := replace(v_queue.id::text, '-', '');
    v_x := 'x_' || v_storage;
    v_p := 'p_' || v_storage;
    v_start := otra.week_bucket_utc(v_now - v_queue.partition_lookback);
    v_end := otra.week_bucket_utc(v_now + v_queue.partition_lookahead);

    if v_queue.default_partition = 'enabled' then
      execute format(
        'create table if not exists otra.%I partition of otra.%I default',
        v_x || '_d', v_x
      );
      execute format(
        'create table if not exists otra.%I partition of otra.%I default',
        v_p || '_d', v_p
      );
    end if;

    v_week := v_start;
    while v_week <= v_end loop
      v_next := v_week + interval '7 days';
      v_tag := otra.partition_week_tag(v_week);
      v_lower := otra.uuid_v7_floor(v_week);
      v_upper := otra.uuid_v7_floor(v_next);
      execute format(
        'create table if not exists otra.%I partition of otra.%I
         for values from (%L::uuid) to (%L::uuid)',
        v_x || '_' || v_tag, v_x, v_lower, v_upper
      );
      execute format(
        'create table if not exists otra.%I partition of otra.%I
         for values from (%L::uuid) to (%L::uuid)',
        v_p || '_' || v_tag, v_p, v_lower, v_upper
      );
      v_week := v_next;
    end loop;
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

-- Raise OT001 unless the worker currently holds the claim on the execution.
-- Guards every history write: a zombie worker whose lease was reaped must
-- not inject side-effect results, timers, event waits, or children into an
-- execution another worker now owns (absurd's AB002 lesson).  Takes the
-- execution row lock, so callers write under it.
create or replace function otra._assert_owner (p_execution uuid, p_worker text) returns void as $$
declare
  v_status text;
  v_claimed_by text;
begin
  select status, claimed_by into v_status, v_claimed_by
    from otra.executions where id = p_execution for update;
  if found and v_status = 'cancelled' then
    -- Killed out from under us: distinct sqlstate so the driver reports
    -- "killed" (an operator action) rather than "lost" (a stolen claim).
    raise exception 'execution % was cancelled', p_execution
      using errcode = 'OT002';
  end if;
  if not found or v_status <> 'running' or v_claimed_by is distinct from p_worker then
    raise exception 'worker % no longer holds the claim on execution %', p_worker, p_execution
      using errcode = 'OT001';
  end if;
end;
$$ language plpgsql;

-- Wake suspended executions (any-resolution wakes; replay re-suspends if the
-- generator is still blocked, which is cheap and race-free).
create or replace function otra._wake (p_execution_ids uuid[]) returns void as $$
begin
  if p_execution_ids is null or array_length(p_execution_ids, 1) is null then
    return;
  end if;
  -- Lock in id order before the bulk update: concurrent wakers touching
  -- overlapping execution sets must acquire row locks in the same order or
  -- they can deadlock (a lesson absurd encodes as its lock-ordering rule).
  perform 1 from otra.executions
    where id = any (p_execution_ids)
    order by id
      for update;
  update otra.executions
     set status = 'pending',
         run_after = otra.now(),
         updated_at = otra.now()
   where id = any (p_execution_ids)
     and status = 'suspended';
  perform pg_notify('otra_wake', queue)
     from (select distinct queue from otra.executions where id = any (p_execution_ids)) q;
end;
$$ language plpgsql;

-- Settle every child-promise that references a finished execution, then wake
-- the (suspended) owners.  This is how results propagate up the tree.
create or replace function otra._settle_child_promises (
  p_child uuid,
  p_resolved boolean,
  p_value jsonb,
  p_error jsonb
) returns void as $$
declare
  v_owners uuid[];
begin
  with settled as (
    update otra.promises
       set status = case when p_resolved then 'resolved' else 'rejected' end,
           value = case when p_resolved then p_value else null end,
           error = case when p_resolved then null else p_error end,
           settled_at = otra.now()
     where child_execution_id = p_child
       and kind = 'child'
       and status = 'pending'
    returning execution_id
  )
  select array_agg(distinct execution_id) into v_owners from settled;
  perform otra._wake(v_owners);
end;
$$ language plpgsql;

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

-- Record one failed attempt: schedule a retry with backoff, or fail the
-- execution permanently (which rejects dependent child-promises so the
-- failure propagates to awaiting parents).
create or replace function otra._fail_attempt (
  p_execution uuid,
  p_error jsonb,
  p_retryable boolean
) returns table (failed_permanently boolean, retry_at timestamptz) as $$
declare
  v_row otra.executions%rowtype;
  v_retry_at timestamptz;
begin
  select * into v_row from otra.executions where id = p_execution for update;
  if not found or v_row.status in ('completed', 'failed', 'cancelled') then
    return query select false, null::timestamptz;
    return;
  end if;

  if p_retryable and v_row.attempt + 1 < v_row.max_attempts then
    -- A poisoned strategy (legacy row predating spawn-time validation) must
    -- fail this execution, never wedge the caller: this runs inside claim()'s
    -- sweep, where an uncaught error would block the whole queue's workers
    -- (absurd's lesson from retry_delay validation).
    begin
      v_retry_at := otra.now() + otra._backoff(v_row.retry_strategy, v_row.attempt + 1);
    exception when others then
      v_retry_at := null;
    end;
  end if;

  if v_retry_at is not null then
    -- A retry with a pending cancel keeps the flag: on the next claim the
    -- driver delivers (or, if '$cancel' is already journaled, re-delivers
    -- at the recorded yield and resumes compensation).  This also covers a
    -- worker dying mid-compensation, via the claim-expiry sweep.
    update otra.executions
       set status = 'pending',
           attempt = attempt + 1,
           run_after = v_retry_at,
           claimed_by = null,
           claim_expires_at = null,
           error = p_error,
           updated_at = otra.now()
     where id = p_execution;
    return query select false, v_retry_at;
  else
    -- claimed_by is deliberately kept on terminal rows: knowing which worker
    -- held the last lease is the forensic trail for double-execution hunts.
    -- An execution with a pending cancel terminates as 'cancelled', never
    -- 'failed': that is the outcome cancellation owns.
    update otra.executions
       set status = case when v_row.cancel_requested_at is not null
                         then 'cancelled' else 'failed' end,
           attempt = attempt + 1,
           claim_expires_at = null,
           error = p_error,
           finished_at = otra.now(),
           updated_at = otra.now()
     where id = p_execution;
    perform otra._settle_child_promises(
      p_execution, false, null,
      case when v_row.cancel_requested_at is not null then
        jsonb_build_object('name', 'CancelledError', 'message', 'execution was cancelled')
      else p_error end);
    return query select true, null::timestamptz;
  end if;
end;
$$ language plpgsql;

------------------------------------------------------------------------------
-- public API
------------------------------------------------------------------------------

-- Spawn an execution.  When spawned from inside another execution
-- (p_parent/p_key given) this is idempotent across replays: the child promise
-- acts as the memo, so re-running the parent never double-spawns.
drop function if exists otra.spawn (text, jsonb, text, jsonb, uuid, text, text);
create or replace function otra.spawn (
  p_function text,
  p_params jsonb default null,
  p_queue text default 'default',
  p_opts jsonb default '{}',
  p_parent uuid default null,
  p_key text default null,
  p_label text default null,
  p_worker text default null
) returns table (execution_id uuid, created boolean) as $$
declare
  v_existing uuid;
  v_id uuid;
  v_root uuid;
  v_delay double precision := coalesce((p_opts ->> 'delay_s')::double precision, 0);
  v_idempotency_key text := p_opts ->> 'idempotency_key';
  v_strategy jsonb := coalesce(p_opts -> 'retry_strategy',
                               '{"kind": "exponential", "base_s": 1, "factor": 2, "max_s": 300}'::jsonb);
begin
  -- Validate the retry strategy now, not at first failure: an invalid one
  -- discovered inside the claim sweep would poison queue maintenance.
  perform otra._backoff(v_strategy, 1);
  -- Top-level idempotent spawn: at-most-one execution per (queue, key).
  -- Race-safe via insert-on-conflict + reread, absurd-style; child spawns
  -- are already deduplicated by the parent's promise key instead.
  if p_parent is null and v_idempotency_key is not null then
    insert into otra.executions
      (queue, function_name, params, max_attempts, retry_strategy, run_after,
       idempotency_key, on_parent_cancel)
    values (
      p_queue, p_function, p_params,
      coalesce((p_opts ->> 'max_attempts')::int, 5),
      v_strategy,
      otra.now() + make_interval(secs => v_delay),
      v_idempotency_key,
      coalesce(p_opts ->> 'on_parent_cancel', 'cascade')
    )
    on conflict (queue, idempotency_key) where idempotency_key is not null do nothing
    returning id into v_id;

    if v_id is null then
      select e.id into v_id from otra.executions e
       where e.queue = p_queue and e.idempotency_key = v_idempotency_key;
      if v_id is null then
        -- The winning insert rolled back between our conflict and reread.
        raise exception 'concurrent idempotent spawn aborted; retry'
          using errcode = '40001';
      end if;
      return query select v_id, false;
      return;
    end if;

    update otra.executions set root_id = v_id where id = v_id;
    perform pg_notify('otra_wake', p_queue);
    return query select v_id, true;
    return;
  end if;

  if p_parent is not null then
    if p_key is null then
      raise exception 'spawn from a parent execution requires a promise key';
    end if;
    -- Only the worker driving the parent may spawn under it.  This both
    -- rejects zombies and serializes concurrent replays, so the
    -- check-then-insert below cannot race itself into orphan children.
    perform otra._assert_owner(p_parent, p_worker);
    select p.child_execution_id into v_existing
      from otra.promises p
     where p.execution_id = p_parent and p.key = p_key;
    if found then
      return query select v_existing, false;
      return;
    end if;
    select root_id into v_root from otra.executions where id = p_parent;
  end if;

  insert into otra.executions
    (queue, function_name, params, parent_id, root_id, max_attempts,
     retry_strategy, run_after, on_parent_cancel)
  values (
    p_queue,
    p_function,
    p_params,
    p_parent,
    v_root,  -- fixed up below for roots
    coalesce((p_opts ->> 'max_attempts')::int, 5),
    v_strategy,
    otra.now() + make_interval(secs => v_delay),
    coalesce(p_opts ->> 'on_parent_cancel', 'cascade')
  )
  returning id into v_id;

  if p_parent is null then
    update otra.executions set root_id = v_id where id = v_id;
  else
    insert into otra.promises (execution_id, key, label, kind, child_execution_id)
    values (p_parent, p_key, coalesce(p_label, p_function), 'child', v_id);
  end if;

  perform pg_notify('otra_wake', p_queue);
  return query select v_id, true;
end;
$$ language plpgsql;

-- Claim up to p_batch runnable executions for a worker.  Doubles as the
-- scheduler sweep: due timers fire, timed-out event waits reject, and expired
-- claims (crashed workers) are converted into failed attempts, all before
-- claiming.  This keeps the system coordinator-free, like absurd.
drop function if exists otra.claim (text, text, double precision, int);
create or replace function otra.claim (
  p_queue text,
  p_worker text,
  p_claim_seconds double precision default 30,
  p_batch int default 1
) returns table (
  execution_id uuid,
  function_name text,
  params jsonb,
  attempt int,
  max_attempts int,
  cancel_requested boolean
) as $$
declare
  v_now timestamptz := otra.now();
  v_woken uuid[];
  v_crashed record;
begin
  if p_claim_seconds is null or p_claim_seconds <= 0 then
    -- A non-positive lease would make the claim instantly sweepable while
    -- the worker runs on believing it owns the execution.
    raise exception 'claim lease must be positive, got %', p_claim_seconds
      using errcode = 'OT003';
  end if;

  -- Sweeps are scoped to this queue (another queue's timers are its own
  -- workers' business), bounded, and SKIP LOCKED so concurrent claimers
  -- don't serialize on the same due rows.  Anything left over is picked up
  -- by the next claim call.

  -- 1. fire due sleep timers
  with due as (
    select p.id
      from otra.promises p
      join otra.executions e on e.id = p.execution_id
     where p.kind = 'sleep' and p.status = 'pending' and p.wake_at <= v_now
       and e.queue = p_queue
     order by p.wake_at
     limit 100
       for update of p skip locked
  ), fired as (
    update otra.promises p
       set status = 'resolved', value = 'null'::jsonb, settled_at = v_now
     where p.id in (select due.id from due)
    returning p.execution_id
  )
  select array_agg(distinct fired.execution_id) into v_woken from fired;
  perform otra._wake(v_woken);

  -- 2. reject timed-out event waits and external promises
  with due as (
    select p.id
      from otra.promises p
      join otra.executions e on e.id = p.execution_id
     where p.kind in ('event', 'external') and p.status = 'pending'
       and p.wake_at is not null and p.wake_at <= v_now
       and e.queue = p_queue
     order by p.wake_at
     limit 100
       for update of p skip locked
  ), timed_out as (
    update otra.promises p
       set status = 'rejected',
           error = case
             when p.kind = 'event' then
               jsonb_build_object('name', 'EventTimeoutError',
                                  'message', 'timed out waiting for event ' || p.event_name)
             else
               jsonb_build_object('name', 'TimeoutError',
                                  'message', 'timed out waiting for external promise "' || p.label || '"')
           end,
           settled_at = v_now
     where p.id in (select due.id from due)
    returning p.execution_id
  )
  select array_agg(distinct timed_out.execution_id) into v_woken from timed_out;
  perform otra._wake(v_woken);

  -- 3. recover crashed workers: an expired claim is a failed attempt
  for v_crashed in
    select e.id
      from otra.executions e
     where e.queue = p_queue and e.status = 'running' and e.claim_expires_at <= v_now
     order by e.claim_expires_at
     limit 100
       for update skip locked
  loop
    perform * from otra._fail_attempt(
      v_crashed.id,
      jsonb_build_object('name', 'ClaimExpiredError',
                         'message', 'worker claim expired before completion'),
      true
    );
  end loop;

  -- 4. claim
  return query
  update otra.executions e
     set status = 'running',
         claimed_by = p_worker,
         claim_expires_at = v_now + make_interval(secs => p_claim_seconds),
         updated_at = v_now
   where e.id in (
           select c.id from otra.executions c
            where c.queue = p_queue and c.status = 'pending' and c.run_after <= v_now
            order by c.run_after, c.id
            limit p_batch
              for update skip locked
         )
  returning e.id, e.function_name, e.params, e.attempt, e.max_attempts,
            (e.cancel_requested_at is not null);
end;
$$ language plpgsql;

-- Load the memoized promise history for replay.  The row id doubles as the
-- external-settlement token for kind = 'external' promises.
drop function if exists otra.load_history (uuid);
create or replace function otra.load_history (p_execution uuid)
returns table (
  id uuid,
  key text,
  label text,
  kind text,
  status text,
  value jsonb,
  error jsonb,
  child_execution_id uuid
) as $$
  select p.id, p.key, p.label, p.kind, p.status, p.value, p.error, p.child_execution_id
    from otra.promises p
   where p.execution_id = p_execution
   order by p.created_at, p.id;
$$ language sql stable;

-- Checkpoint a completed side effect ("run").  Write-once: on replay races
-- (two workers overlapping on the same execution) the first write wins and
-- the canonical value is returned.  Also extends the caller's claim, so
-- steady checkpoint progress keeps an execution owned.
create or replace function otra.record_run (
  p_execution uuid,
  p_worker text,
  p_key text,
  p_label text,
  p_value jsonb,
  p_claim_seconds double precision default 30
) returns jsonb as $$
declare
  v_value jsonb;
begin
  perform otra._assert_owner(p_execution, p_worker);
  insert into otra.promises (execution_id, key, label, kind, status, value, settled_at)
  values (p_execution, p_key, p_label, 'run', 'resolved', p_value, otra.now())
  on conflict (execution_id, key) do nothing;

  select value into v_value
    from otra.promises
   where execution_id = p_execution and key = p_key;

  update otra.executions
     set claim_expires_at = otra.now() + make_interval(secs => p_claim_seconds),
         updated_at = otra.now()
   where id = p_execution and claimed_by = p_worker and status = 'running';

  return v_value;
end;
$$ language plpgsql;

-- Create (or fetch) a sleep-timer promise.
drop function if exists otra.create_sleep (uuid, text, text, double precision);
create or replace function otra.create_sleep (
  p_execution uuid,
  p_worker text,
  p_key text,
  p_label text,
  p_seconds double precision
) returns table (status text, value jsonb, error jsonb) as $$
begin
  perform otra._assert_owner(p_execution, p_worker);
  insert into otra.promises (execution_id, key, label, kind, wake_at)
  values (p_execution, p_key, p_label, 'sleep', otra.now() + make_interval(secs => p_seconds))
  on conflict (execution_id, key) do nothing;
  return query
    select p.status, p.value, p.error
      from otra.promises p
     where p.execution_id = p_execution and p.key = p_key;
end;
$$ language plpgsql;

-- Create (or fetch) an event-wait promise.  If a matching event was already
-- emitted, the promise is born resolved -- this is what makes event waits
-- race-free: emit-then-await and await-then-emit both work.
drop function if exists otra.create_event_wait (uuid, text, text, text, double precision);
create or replace function otra.create_event_wait (
  p_execution uuid,
  p_worker text,
  p_key text,
  p_label text,
  p_event_name text,
  p_timeout_seconds double precision default null
) returns table (status text, value jsonb, error jsonb) as $$
declare
  v_queue text;
  v_cached record;
begin
  select queue into v_queue from otra.executions where id = p_execution;

  -- Serialize against emit_event on this (queue, event name).  Without a
  -- shared lock, an emit landing between our cache read and our promise
  -- insert sees no waiter while we see no event: the wakeup is lost and an
  -- untimed wait hangs forever (absurd's event race, commit bcde0df / #61).
  perform pg_advisory_xact_lock(
    hashtextextended('otra:event:' || v_queue || ':' || p_event_name, 0));
  perform otra._assert_owner(p_execution, p_worker);

  -- Facts are unique per (queue, name): at most one row can match.
  select ev.payload into v_cached
    from otra.events ev
   where ev.queue = v_queue and ev.name = p_event_name;

  if found then
    insert into otra.promises
      (execution_id, key, label, kind, status, value, event_name, settled_at)
    values (p_execution, p_key, p_label, 'event', 'resolved', v_cached.payload,
            p_event_name, otra.now())
    on conflict (execution_id, key) do nothing;
  else
    insert into otra.promises (execution_id, key, label, kind, event_name, wake_at)
    values (p_execution, p_key, p_label, 'event', p_event_name,
            case when p_timeout_seconds is null then null
                 else otra.now() + make_interval(secs => p_timeout_seconds) end)
    on conflict (execution_id, key) do nothing;
  end if;

  return query
    select p.status, p.value, p.error
      from otra.promises p
     where p.execution_id = p_execution and p.key = p_key;
end;
$$ language plpgsql;

-- Create (or fetch) an externally-settleable promise (kind 'external').
-- Its row id is the settlement token the task hands to the outside world;
-- app.resolvePromise / rejectPromise settle it by that id.  Optional
-- timeout rides the same wake_at rejection sweep as event waits.
create or replace function otra.create_external (
  p_execution uuid,
  p_worker text,
  p_key text,
  p_label text,
  p_timeout_seconds double precision default null
) returns table (id uuid, status text, value jsonb, error jsonb) as $$
begin
  perform otra._assert_owner(p_execution, p_worker);
  insert into otra.promises (execution_id, key, label, kind, wake_at)
  values (p_execution, p_key, p_label, 'external',
          case when p_timeout_seconds is null then null
               else otra.now() + make_interval(secs => p_timeout_seconds) end)
  on conflict (execution_id, key) do nothing;
  return query
    select p.id, p.status, p.value, p.error
      from otra.promises p
     where p.execution_id = p_execution and p.key = p_key;
end;
$$ language plpgsql;

-- Settle an external promise by its token (row id), from regular code.
-- Write-once: an already-settled promise returns false, as does any id that
-- is not an external promise -- outside code may only settle promises the
-- owning task explicitly handed out.  Lock order matches every other
-- resolver: promise row first, execution row second (via _wake), so the
-- suspend race protocol applies unchanged.
create or replace function otra.resolve_promise (
  p_id uuid,
  p_value jsonb
) returns boolean as $$
declare
  v_owner uuid;
begin
  update otra.promises p
     set status = 'resolved', value = p_value, settled_at = otra.now()
   where p.id = p_id and p.kind = 'external' and p.status = 'pending'
  returning p.execution_id into v_owner;
  if not found then
    return false;
  end if;
  perform otra._wake(array[v_owner]);
  return true;
end;
$$ language plpgsql;

create or replace function otra.reject_promise (
  p_id uuid,
  p_error jsonb
) returns boolean as $$
declare
  v_owner uuid;
begin
  update otra.promises p
     set status = 'rejected', error = p_error, settled_at = otra.now()
   where p.id = p_id and p.kind = 'external' and p.status = 'pending'
  returning p.execution_id into v_owner;
  if not found then
    return false;
  end if;
  perform otra._wake(array[v_owner]);
  return true;
end;
$$ language plpgsql;

-- Refresh the current status of a set of promises (used by the SDK right
-- before deciding to suspend, and to fast-path awaits on stale history).
drop function if exists otra.get_promises (uuid, text[]);
create or replace function otra.get_promises (p_execution uuid, p_keys text[])
returns table (id uuid, key text, kind text, status text, value jsonb, error jsonb, child_execution_id uuid) as $$
  select p.id, p.key, p.kind, p.status, p.value, p.error, p.child_execution_id
    from otra.promises p
   where p.execution_id = p_execution and p.key = any (p_keys);
$$ language sql stable;

-- Suspend an execution that is blocked on the given promise keys.  Refuses
-- (suspended = false) if any blocker has already settled -- the worker just
-- replays -- or if a cancel is pending (cancel_requested = true) -- the
-- worker delivers CancelledError instead of parking an execution nothing
-- would ever wake.  The execution row is locked before the blockers are
-- checked; resolvers lock the promise row first and the execution row
-- second, so a concurrent resolution either lands before the check (we
-- refuse to suspend) or blocks until we commit (its conditional wake then
-- sees status = 'suspended').  No lost wakeups either way.
drop function if exists otra.suspend (uuid, text, text[]);
create or replace function otra.suspend (
  p_execution uuid,
  p_worker text,
  p_blocker_keys text[]
) returns table (suspended boolean, cancel_requested boolean) as $$
declare
  v_status text;
  v_claimed_by text;
  v_cancel timestamptz;
begin
  select e.status, e.claimed_by, e.cancel_requested_at
    into v_status, v_claimed_by, v_cancel
    from otra.executions e where e.id = p_execution for update;
  if not found or v_status <> 'running' or v_claimed_by is distinct from p_worker then
    return query select false, false;
    return;
  end if;

  -- A pending cancel blocks parking only BEFORE delivery (the driver must
  -- deliver instead).  Once the '$cancel' journal row exists, suspension is
  -- legal again: that is what lets compensation call children and sleep.
  if v_cancel is not null and not exists (
    select 1 from otra.promises p2
     where p2.execution_id = p_execution and p2.key = '$cancel'
  ) then
    return query select false, true;
    return;
  end if;

  if exists (
    select 1 from otra.promises p
     where p.execution_id = p_execution
       and p.key = any (p_blocker_keys)
       and p.status <> 'pending'
  ) then
    return query select false, false;
    return;
  end if;

  update otra.executions
     set status = 'suspended',
         claimed_by = null,
         claim_expires_at = null,
         updated_at = otra.now()
   where id = p_execution;
  return query select true, false;
end;
$$ language plpgsql;

-- Complete an execution and propagate the result to awaiting parents.
create or replace function otra.complete (
  p_execution uuid,
  p_worker text,
  p_result jsonb
) returns void as $$
begin
  -- Insurance: once cancellation has been delivered (journaled as $cancel),
  -- the only legal terminal transition is finalize_cancelled.
  if exists (
    select 1 from otra.promises p
     where p.execution_id = p_execution and p.key = '$cancel'
  ) then
    raise exception 'execution % has a delivered cancellation; it can only finalize as cancelled',
      p_execution;
  end if;
  -- claimed_by survives on terminal rows as the forensic record of which
  -- worker finished the execution.
  update otra.executions
     set status = 'completed',
         result = p_result,
         claim_expires_at = null,
         finished_at = otra.now(),
         updated_at = otra.now()
   where id = p_execution and claimed_by = p_worker and status = 'running';
  if not found then
    raise exception 'execution % is not running under worker %', p_execution, p_worker;
  end if;
  perform otra._settle_child_promises(p_execution, true, p_result, null);
end;
$$ language plpgsql;

-- Report a failed attempt (uncaught error during replay).  Guarded by
-- ownership: a zombie worker whose lease was stolen must not knock a live
-- worker's execution back to pending (applied = false tells it to abandon).
-- The unguarded _fail_attempt is reserved for the claim sweep, which selects
-- the expired rows itself.
drop function if exists otra.fail_attempt (uuid, text, jsonb, boolean);
create or replace function otra.fail_attempt (
  p_execution uuid,
  p_worker text,
  p_error jsonb,
  p_retryable boolean default true
) returns table (applied boolean, failed_permanently boolean, retry_at timestamptz) as $$
declare
  v_result record;
begin
  perform 1 from otra.executions
    where id = p_execution and claimed_by = p_worker and status = 'running'
      for update;
  if not found then
    return query select false, false, null::timestamptz;
    return;
  end if;
  select * into v_result from otra._fail_attempt(p_execution, p_error, p_retryable);
  return query select true, v_result.failed_permanently, v_result.retry_at;
end;
$$ language plpgsql;

-- Emit an event: cache it and settle any pending event-waits with that name
-- in the same queue, waking their owners.
create or replace function otra.emit_event (
  p_queue text,
  p_name text,
  p_payload jsonb default null
) returns void as $$
declare
  v_owners uuid[];
  v_event_id uuid;
begin
  -- Counterpart of the lock in create_event_wait: see the comment there.
  perform pg_advisory_xact_lock(
    hashtextextended('otra:event:' || p_queue || ':' || p_name, 0));
  -- First write wins: a repeat emit of an existing fact is a no-op -- it
  -- neither changes the payload nor re-runs the wakeup cascade.
  insert into otra.events (queue, name, payload)
  values (p_queue, p_name, p_payload)
  on conflict (queue, name) do nothing
  returning id into v_event_id;
  if v_event_id is null then
    return;
  end if;

  with woken as (
    update otra.promises p
       set status = 'resolved', value = p_payload, settled_at = otra.now()
      from otra.executions e
     where p.execution_id = e.id
       and e.queue = p_queue
       and p.kind = 'event'
       and p.status = 'pending'
       and p.event_name = p_name
    returning p.execution_id
  )
  select array_agg(distinct execution_id) into v_owners from woken;
  perform otra._wake(v_owners);
end;
$$ language plpgsql;

-- Push a claimed execution back to pending without consuming an attempt.
-- Used by workers that claim an execution they have no handler for (e.g.
-- during a rolling deploy).
create or replace function otra.defer (
  p_execution uuid,
  p_worker text,
  p_delay_seconds double precision default 15
) returns boolean as $$
begin
  update otra.executions
     set status = 'pending',
         run_after = otra.now() + make_interval(secs => p_delay_seconds),
         claimed_by = null,
         claim_expires_at = null,
         updated_at = otra.now()
   where id = p_execution and claimed_by = p_worker and status = 'running';
  return found;
end;
$$ language plpgsql;

-- Heartbeat for long-running local side effects.  The return value doubles
-- as the cancellation discovery channel (Temporal delivers activity
-- cancellation on the heartbeat response for the same reason): held = the
-- claim is still ours, cancel_requested = a graceful cancel is pending.
drop function if exists otra.extend_claim (uuid, text, double precision);
create or replace function otra.extend_claim (
  p_execution uuid,
  p_worker text,
  p_claim_seconds double precision default 30
) returns table (held boolean, cancel_requested boolean) as $$
declare
  v_cancel timestamptz;
begin
  update otra.executions e
     set claim_expires_at = otra.now() + make_interval(secs => p_claim_seconds),
         updated_at = otra.now()
   where e.id = p_execution and e.claimed_by = p_worker and e.status = 'running'
  returning e.cancel_requested_at into v_cancel;
  if not found then
    return query select false, false;
  else
    return query select true, v_cancel is not null;
  end if;
end;
$$ language plpgsql;

-- Graceful cancellation: record a request against each live execution in
-- the tree and make sure a worker will look at it.  Per target:
--   terminal                  -> noop
--   pending, empty history    -> finalize now (nothing to compensate)
--   suspended                 -> wake it; a worker replays and delivers
--   running / pending w. hist -> flag only; heartbeat or claim delivers
-- Cascade walks parent_id top-down, pruning children spawned with
-- on_parent_cancel = 'detach', locking rows in id order (deadlock rule).
drop function if exists otra.cancel (uuid);
create or replace function otra.request_cancel (
  p_execution uuid,
  p_cascade boolean default true,
  p_reason text default null
) returns table (execution_id uuid, action text) as $$
declare
  v_now timestamptz := otra.now();
  v_row record;
begin
  for v_row in
    with recursive tree as (
      select e2.id from otra.executions e2 where e2.id = p_execution
      union all
      select c.id
        from otra.executions c
        join tree t on c.parent_id = t.id
       where p_cascade and c.on_parent_cancel = 'cascade'
    )
    select e.id, e.status, e.queue,
           exists (select 1 from otra.promises pr where pr.execution_id = e.id) as has_history
      from otra.executions e
     where e.id in (select t2.id from tree t2)
     order by e.id
       for update of e
  loop
    if v_row.status in ('completed', 'failed', 'cancelled') then
      execution_id := v_row.id; action := 'noop'; return next;
      continue;
    end if;

    update otra.executions e
       set cancel_requested_at = coalesce(e.cancel_requested_at, v_now),
           cancel_reason = coalesce(e.cancel_reason, p_reason),
           updated_at = v_now
     where e.id = v_row.id;

    if v_row.status = 'pending' and not v_row.has_history then
      -- Never ran: nothing to compensate, finalize in place.
      update otra.executions e
         set status = 'cancelled',
             error = jsonb_build_object('name', 'CancelledError',
                                        'message', coalesce(p_reason, 'execution was cancelled')),
             finished_at = v_now,
             updated_at = v_now
       where e.id = v_row.id;
      perform otra._settle_child_promises(
        v_row.id, false, null,
        jsonb_build_object('name', 'CancelledError', 'message', 'execution was cancelled'));
      execution_id := v_row.id; action := 'cancelled'; return next;
    elsif v_row.status = 'suspended' then
      update otra.executions e
         set status = 'pending', run_after = v_now, updated_at = v_now
       where e.id = v_row.id;
      perform pg_notify('otra_wake', v_row.queue);
      execution_id := v_row.id; action := 'woken'; return next;
    else
      execution_id := v_row.id; action := 'requested'; return next;
    end if;
  end loop;
end;
$$ language plpgsql;

-- The escape hatch: immediate termination, no compensation.  The driving
-- worker discovers OT002 at its next guarded write and abandons.  Restate's
-- `cancel --kill`; Temporal's terminate.
create or replace function otra.kill (
  p_execution uuid,
  p_cascade boolean default true,
  p_reason text default null
) returns int as $$
declare
  v_now timestamptz := otra.now();
  v_row record;
  v_count int := 0;
begin
  for v_row in
    with recursive tree as (
      select e2.id from otra.executions e2 where e2.id = p_execution
      union all
      select c.id
        from otra.executions c
        join tree t on c.parent_id = t.id
       where p_cascade and c.on_parent_cancel = 'cascade'
    )
    select e.id, e.status
      from otra.executions e
     where e.id in (select t2.id from tree t2)
     order by e.id
       for update of e
  loop
    if v_row.status in ('completed', 'failed', 'cancelled') then
      continue;
    end if;
    -- claimed_by is kept: the forensic record of who held the lease.
    update otra.executions e
       set status = 'cancelled',
           cancel_requested_at = coalesce(e.cancel_requested_at, v_now),
           cancel_reason = coalesce(e.cancel_reason, p_reason),
           claim_expires_at = null,
           error = jsonb_build_object('name', 'CancelledError',
                                      'message', coalesce(p_reason, 'execution was killed')),
           finished_at = v_now,
           updated_at = v_now
     where e.id = v_row.id;
    perform otra._settle_child_promises(
      v_row.id, false, null,
      jsonb_build_object('name', 'CancelledError', 'message', 'execution was cancelled'));
    v_count := v_count + 1;
  end loop;
  return v_count;
end;
$$ language plpgsql;

-- Journal the delivery of a graceful cancel: a promise row (kind 'cancel',
-- key '$cancel') recording WHERE CancelledError was thrown into the
-- generator, so compensation may suspend and every replay re-delivers at
-- the same yield.  Write-once; returns the canonical recorded position
-- (first write wins, e.g. across a crash between record and throw).
create or replace function otra.record_cancel (
  p_execution uuid,
  p_worker text,
  p_position jsonb
) returns jsonb as $$
declare
  v_position jsonb;
begin
  perform otra._assert_owner(p_execution, p_worker);
  insert into otra.promises (execution_id, key, label, kind, status, value, settled_at)
  values (p_execution, '$cancel', '$cancel', 'cancel', 'resolved', p_position, otra.now())
  on conflict (execution_id, key) do nothing;
  select p.value into v_position
    from otra.promises p
   where p.execution_id = p_execution and p.key = '$cancel';
  return v_position;
end;
$$ language plpgsql;

-- Terminal transition after a delivered graceful cancel.  Ownership-guarded:
-- only the worker driving the compensation may finalize.  The engine owns
-- the outcome -- callers reach this however the generator ended (returned,
-- rethrew CancelledError, or threw something else, recorded via p_error).
create or replace function otra.finalize_cancelled (
  p_execution uuid,
  p_worker text,
  p_error jsonb default null
) returns boolean as $$
begin
  update otra.executions e
     set status = 'cancelled',
         claim_expires_at = null,
         error = coalesce(p_error, e.error),
         finished_at = otra.now(),
         updated_at = otra.now()
   where e.id = p_execution
     and e.claimed_by = p_worker
     and e.status = 'running'
     and e.cancel_requested_at is not null;
  if not found then
    return false;
  end if;
  perform otra._settle_child_promises(
    p_execution, false, null,
    jsonb_build_object('name', 'CancelledError', 'message', 'execution was cancelled'));
  return true;
end;
$$ language plpgsql;

drop function if exists otra.get_execution (uuid);
create or replace function otra.get_execution (p_execution uuid)
returns table (
  id uuid, queue text, function_name text, status text, attempt int,
  params jsonb, result jsonb, error jsonb,
  parent_id uuid, root_id uuid,
  cancel_requested_at timestamptz, cancel_reason text,
  created_at timestamptz, finished_at timestamptz
) as $$
  select e.id, e.queue, e.function_name, e.status, e.attempt,
         e.params, e.result, e.error,
         e.parent_id, e.root_id,
         e.cancel_requested_at, e.cancel_reason,
         e.created_at, e.finished_at
    from otra.executions e
   where e.id = p_execution;
$$ language sql stable;

-- Delete finished execution trees and stale events older than the TTL.
-- Bounded per call (absurd bounds cleanup for the same reason it bounds the
-- claim sweep), and a tree is only eligible once every descendant is
-- terminal: a completed parent may have fire-and-forget children still
-- running, and the parent_id cascade would otherwise delete live work.
drop function if exists otra.cleanup (interval);
create or replace function otra.cleanup (p_ttl interval, p_limit int default 1000) returns void as $$
begin
  delete from otra.executions e
   where e.id in (
     select r.id
       from otra.executions r
      where r.status in ('completed', 'failed', 'cancelled')
        and r.parent_id is null
        and r.finished_at < otra.now() - p_ttl
        and not exists (
          select 1 from otra.executions d
           where d.root_id = r.id
             and d.status not in ('completed', 'failed', 'cancelled')
        )
      order by r.finished_at
      limit p_limit
   );
  delete from otra.events e
   where e.id in (
     select ev.id from otra.events ev
      where ev.created_at < otra.now() - p_ttl
      order by ev.created_at
      limit p_limit
   );
end;
$$ language plpgsql;
