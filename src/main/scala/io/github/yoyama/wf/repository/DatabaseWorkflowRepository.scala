package io.github.yoyama.wf.repository

import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun}

import cats._
import cats.implicits._
import scalikejdbc._
import scalikejdbc.config._

class DatabaseWorkflowRepository extends WorkflowRepository {
  
  def getWorkflowRun(id: Int): Transaction[Option[WorkflowRun]] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      WorkflowRun.find(id)(session)
    }
  }

  def getTaskRun(wfid: Int): Transaction[Seq[TaskRun]] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      TaskRun.findAllBy(sqls"wfid = ${wfid}")(session)
    }
  }

  def getLinkRun(wfid: Int): Transaction[Seq[LinkRun]] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      LinkRun.findAllBy(sqls"wfid = ${wfid}")(session)
    }
  }

  def assignNewWfId(): Transaction[Int] = {
    ScalikeJDBCTransaction.from { (session: DBSession) =>
      implicit val s = session
      sql"""select nextval('running.workflow_id')""".map(rs => rs.int("nextval")).single.apply().get
    }
  }

  // Save a workflow to all related tables. if wfId is None, assign new ID.
  def saveNewWorkflowRunAll(wf: WorkflowRun, tasks: Seq[TaskRun], links: Seq[LinkRun],
                            wfid: Option[Int] = None): Transaction[(WorkflowRun, Seq[TaskRun], Seq[LinkRun])] = {
    for {
      id <- wfid.map(i => ScalikeJDBCTransaction.from(i)).getOrElse(assignNewWfId())
      wf2 <- ScalikeJDBCTransaction.from(ss => wf.copy(id = id).save()(ss))
      tasks2 <- tasks
        .map(t => ScalikeJDBCTransaction.from(ss => t.copy(wfid = id).save()(ss)))
        .toList.traverse(identity)
      links2 <- links
        .map(t => ScalikeJDBCTransaction.from(ss => t.copy(wfid = id).save()(ss)))
        .toList.traverse(identity)
    } yield (wf2, tasks2, links2)
  }

  // Update a workflow to all related tables. Existing records are deleted then inserted.
  def updateWorkflowRunAll(wf: WorkflowRun, task: Seq[TaskRun], links: Seq[LinkRun]): Transaction[(WorkflowRun, Seq[TaskRun], Seq[LinkRun])] = {
    ???
  }
}
