package io.github.yoyama.wf.workflow

import java.time.Instant
import io.github.yoyama.wf.{RunID, TaskID}
import io.github.yoyama.wf.dag.Dag
import io.github.yoyama.wf.tag.Tag
import io.github.yoyama.wf.workflow.TaskState.{INITIALIZING, POST_PROCESSING, PROVISIONING, READY, RUNNING, STOP, STOPPING, WAIT}

enum WorkflowState(val value:Int) {
  case WAIT extends WorkflowState(0)
  case READY extends WorkflowState(1)
  case RUNNING extends WorkflowState(21)
  case CANCELLING extends WorkflowState(98)
  case STOPPING extends WorkflowState(98)
  case STOP extends WorkflowState(99)
}

object WorkflowState {
  def apply(v:Int):WorkflowState = v match {
    case 0 => WAIT
    case 1 => READY
    case 21 => RUNNING
    case 91 => CANCELLING
    case 98 => STOPPING
    case 99 => STOP
    case _ => throw new RuntimeException(s"Invalid workflow state value:${v}")
  }
}

// A dag for workflow
case class WorkflowDag(id:RunID, name:String, dag:Dag, tasks:Map[TaskID,WorkflowTask],
                       state:WorkflowState = WorkflowState.WAIT, startAt:Option[Instant] = None, finishAt:Option[Instant] = None,
                       createdAt: Option[Instant] = None, updatedAt: Option[Instant] = None,
                       tags:Tag = Tag()) {
  def getTask(id:TaskID):Option[WorkflowTask] = tasks.get(id)
  def getParents(id:TaskID):Seq[TaskID] = dag.parents.get(id).map(_.toSeq).getOrElse(Seq.empty)
  def getChildren(id:TaskID):Seq[TaskID] = dag.children.get(id).map(_.toSeq).getOrElse(Seq.empty)

  def printInfo: String = {
    val sb = new StringBuilder()
    sb.append(s"Workflow ID: ${id}  ")
    sb.append(s"root: id ${dag.root.id}  ")
    sb.append(s"terminal: id ${dag.terminal.id}\n")
    sb.append(s"tasks: ")
    sb.append(tasks.toSeq.sortBy(x => x._1).map(x => s"${x._2.id} ${x._2.name}").mkString(","))
    sb.toString()
  }
}
