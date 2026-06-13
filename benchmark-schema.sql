create type task_status as enum ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED');

create table benchmark_tasks (
    id bigint generated always as identity primary key,
    payload jsonb not null,
    status task_status not null default 'PENDING',
    retry_count int not null default 0,
    created_at timestamptz not null default NOW(),
    locked_at timestamptz,
    updated_at timestamptz not null default NOW()
);

create index idx_benchmark_tasks_status on benchmark_tasks(status)
where status = 'PENDING';