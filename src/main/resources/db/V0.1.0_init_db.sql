-- create schema definition;

create schema running;

create table running.workflow_run(
    run_id int,
    name varchar not null,
    state int not null,
    start_at timestamp,
    finish_at timestamp,
    tags jsonb,
    created_at timestamp not null,
    updated_at timestamp not null,
    primary key(run_id)
);

create table running.task_run(
    task_id int,
    run_id int not null,
    name varchar not null,
    type varchar not null,
    config jsonb not null, -- task definition
    state int not null,
    input_params jsonb, -- parameters which are passed to the operator when start
    output_params jsonb, -- parameters which are stored and pass to the net tasks when finish
    system_params jsonb, -- read only parameters from workflow system to the task
    state_params jsonb, -- parameters while running.
    next_poll timestamp, -- polling time while running
    result int,
    err_code int,
    start_at timestamp,
    finish_at timestamp,
    tags jsonb,
    created_at timestamp not null,
    updated_at timestamp not null,
    primary key(task_id, run_id),
    FOREIGN KEY(run_id) references running.workflow_run(run_id)
);

create table running.link_run(
    run_id int not null,
    parent int not null,
    child int not null,
    created_at timestamp not null,
    primary key(run_id, parent, child),
    FOREIGN KEY(run_id) references running.workflow_run(run_id),
    FOREIGN KEY(parent, run_id) references running.task_run(task_id, run_id),
    FOREIGN KEY(child, run_id) references running.task_run(task_id, run_id)
);

create sequence running.run_id;
