create schema definition;

create schema running;

create table running.workflow_run(
    id int,
    name varchar not null,
    state int not null,
    start_at timestamp,
    finish_at timestamp,
    tag jsonb,
    created_at timestamp not null,
    updated_at timestamp not null,
    primary key(id)
);

create table running.task_run(
    id int,
    wfid int not null,
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
    tag jsonb,
    created_at timestamp not null,
    updated_at timestamp not null,
    primary key(id, wfid),
    FOREIGN KEY(wfid) references running.workflow_run(id)
);

create table running.link_run(
    id int,
    wfid int not null,
    parent int not null,
    child int not null,
    created_at timestamp not null,
    primary key(id),
    FOREIGN KEY(wfid) references running.workflow_run(id),
    FOREIGN KEY(parent, wfid) references running.task_run(id, wfid),
    FOREIGN KEY(child, wfid) references running.task_run(id, wfid)
);
create sequence running.workflow_id;
create sequence running.task_id;
create sequence running.link_id;
