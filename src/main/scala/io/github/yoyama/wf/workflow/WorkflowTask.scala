package io.github.yoyama.wf.workflow

import io.github.yoyama.wf.TaskID

import java.time.Instant

// A task of Dag for workflow
// state: 0:wait 1:ready 5:provisioning 11:initializing 21:running 31:post processing 98:stopping 99:stop
case class WorkflowTask(id:TaskID, name: String, tType: String, config: String, state: Int = 0,
                        result: Option[Int] = None, errorCode: Option[Int] = None,
                        startAt: Option[Instant] = None, finishAt: Option[Instant] = None, tags:Map[String,String] = Map.empty,
                        createdAt: Instant,
                        updatedAt: Instant
                       )
