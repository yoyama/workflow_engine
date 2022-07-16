package io.github.yoyama.wf.workflow

import cats.implicits.*

import io.github.yoyama.utils.OptionHelper.*
import io.github.yoyama.wf.{RunID, TaskID}
import io.github.yoyama.wf.dag.{Dag, DagCell, LinkMap}
import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun}
import io.github.yoyama.wf.repository.{Transaction, TransactionRunner, WorkflowRunRepository}
import io.github.yoyama.wf.tag.Tag

import java.time.Instant
import scala.util.{Success, Try}

class WorkflowDagBuilder (val wfRepo:WorkflowRunRepository)(implicit val tRunner:TransactionRunner) {
  // Create WorkflowDag by loading data in DB
  def loadWorkflowDag(wfid: RunID): Try[WorkflowDag] = {
    val transaction: Transaction[(WorkflowRun, Seq[TaskRun], Seq[LinkRun])] = for {
      wf <- wfRepo.getWorkflowRun(wfid)
      t <- wfRepo.getTaskRun(wfid)
      l <- wfRepo.getLinkRun(wfid)
    } yield (wf, t, l)
    for {
      tResult <- transaction.run.v.toTry
      wfRun: WorkflowRun <- Success(tResult._1)
      wf <- buildWorkflowDag(wfRun, tResult._2, tResult._3)
    } yield wf
  }

  def toWorkflowTask(tr:TaskRun): Try[WorkflowTask] = {
    for {
      tags <- Tag.from(tr.tags)
    } yield WorkflowTask(
      id = tr.taskId,
      runId = tr.runId,
      name = tr.name,
      tType = tr.`type`,
      config = tr.config,
      state = TaskState(tr.state),
      result = tr.result,
      errorCode = tr.errCode,
      startAt = tr.startAt,
      finishAt = tr.finishAt,
      createdAt = tr.createdAt,
      updatedAt = tr.updatedAt,
      tags = tags
    )
  }

  // Build WorkflowDag from model data
  def buildWorkflowDag(wfR: WorkflowRun, tasksR: Seq[TaskRun], linksR: Seq[LinkRun]): Try[WorkflowDag] = {

    def convLink(lr: Seq[LinkRun]): Try[Seq[(TaskID, TaskID)]] = {
      val ret = lr.map(l => (l.parent, l.child))
      Success(ret)
    }

    for {
      tasks: Seq[WorkflowTask] <- tasksR.toList.traverse(toWorkflowTask)
      links: Seq[(TaskID, TaskID)] <- convLink(linksR)
      wfTag: Tag <- Tag.from(wfR.tags)
      wf <- buildWorkflowDag(wfR.runId, wfR.name, tasks, links, tags = wfTag)
    } yield wf
  }

  // Build WorkflowDag from WorkflowTask and task link (from, to)
  def buildWorkflowDag(id: RunID, name: String, wfTasks: Seq[WorkflowTask], pairs: Seq[(TaskID, TaskID)], tags: Tag): Try[WorkflowDag] = {
    def convLink(pairs: Seq[(TaskID, TaskID)]): Try[(LinkMap, LinkMap)] = {
      val ret = pairs.foldLeft((Map.empty[Int, Set[Int]], Map.empty[Int, Set[Int]])) { (acc, c) =>
        val (pLink: LinkMap, cLink: LinkMap) = acc
        val (pid: TaskID, cid: TaskID) = c
        val newPLink: LinkMap = pLink.get(cid) match {
          case Some(values) => pLink.updated(cid, values + pid)
          case None => pLink.updated(cid, Set(pid))
        }
        val newCLink: LinkMap = cLink.get(pid) match {
          case Some(values) => cLink.updated(pid, values + cid)
          case None => cLink.updated(pid, Set(cid))
        }
        (newPLink, newCLink)
      }
      Success(ret)
    }

    def convCells(tasks: Seq[WorkflowTask]): Try[Map[TaskID, DagCell]] = {
      Success(
        tasks
          .map(t => (t.id, DagCell(t.id, t.state.value, Instant.now())))
          .toMap
      )
    }

    for {
      cells <- convCells(wfTasks)
      (plinks, clinks) <- convLink(pairs)
      root <- cells.get(0).toTry("No root cell")
      terminal <- cells.get(-1).toTry("No terminal cell")
      dag <- Success(Dag(root, terminal, cells, plinks, clinks))
      id2wftasks <- Success(wfTasks.map(t => (t.id, t)).toMap)
    } yield WorkflowDag(id, name, dag, id2wftasks, tags = tags)
  }
}
