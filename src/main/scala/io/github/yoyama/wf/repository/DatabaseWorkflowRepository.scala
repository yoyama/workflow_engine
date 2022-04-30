package io.github.yoyama.wf.repository

import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun}
import io.github.yoyama.wf.db.model.running.WorkflowRunAll

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

  // Create a workflow to all related tables. if wfId is None, assign new ID.
  def saveNewWorkflowRunAll(wfa: WorkflowRunAll, wfId: Option[Int] = None): Transaction[WorkflowRunAll] = {
    for {
      wfId2 <- wfId.map(i => ScalikeJDBCTransaction.from(i)).getOrElse(assignNewWfId())
      wf2 <- ScalikeJDBCTransaction.from(ss => {
        WorkflowRun.create(wfa.wf.copy(id = wfId2))(ss)
      })
      tasks2 <- wfa.tasks
        .map(t => ScalikeJDBCTransaction.from(ss =>  TaskRun.create(t.copy(wfid = wfId2))(ss)))
        .toList.traverse(identity)
      links2 <- wfa.links
        .map(l => ScalikeJDBCTransaction.from(ss => LinkRun.create(l.copy(wfid = wfId2))(ss)))
        .toList.traverse(identity)
    } yield WorkflowRunAll(wf2, tasks2, links2)
  }

  // Update a workflow to all related tables. Existing records are deleted then inserted.
  def updateWorkflowRunAll(wfa: WorkflowRunAll): Transaction[WorkflowRunAll] = {
    ???
  }
}
