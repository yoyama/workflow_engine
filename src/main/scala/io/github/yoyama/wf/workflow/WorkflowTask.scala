package io.github.yoyama.wf.workflow

import io.github.yoyama.wf.tag.Tag
import io.github.yoyama.wf.{RunID, TaskID}

import java.time.Instant


enum TaskState(val value:Int) {
  case WAIT extends TaskState(0)
  case READY extends TaskState(1)
  case PROVISIONING extends TaskState(5)
  case INITIALIZING extends TaskState(11)
  case RUNNING extends TaskState(21)
  case POST_PROCESSING extends TaskState(31)
  case STOPPING extends TaskState(98)
  case STOP extends TaskState(99)
}

object TaskState {
  def apply(v:Int):TaskState = v match {
    case 0 => WAIT
    case 1 => READY
    case 5 => PROVISIONING
    case 11 => INITIALIZING
    case 21 => RUNNING
    case 31 => POST_PROCESSING
    case 98 => STOPPING
    case 99 => STOP
    case _ => throw new RuntimeException(s"Invalid task state value:${v}")
  }
}
// A task of Dag for workflow
// state: 0:wait 1:ready 5:provisioning 11:initializing 21:running 31:post processing 98:stopping 99:stop
case class WorkflowTask(id:TaskID, runId:RunID, name: String, tType: String, config: String, 
                        state: TaskState = TaskState.WAIT, result: Option[Int] = None, errorCode: Option[Int] = None,
                        startAt: Option[Instant] = None, finishAt: Option[Instant] = None, tags:Tag = Tag(),
                        createdAt: Instant,
                        updatedAt: Instant
                       )
