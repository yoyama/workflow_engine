package io.github.yoyama.wf.workflow

import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun}
import io.github.yoyama.wf.repository.{DatabaseWorkflowRunRepository, Transaction, TransactionResult, TransactionRunner, WorkflowRunRepository}
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowDagOpsTest extends AnyFlatSpec {
  implicit val tRunner: TransactionRunner = new TransactionRunner {
    override def run[A](transaction: Transaction[A]): TransactionResult[A] = ???
  }

  val wfRepo = new DatabaseWorkflowRunRepository
  val wfops = new WorkflowDagOps(wfRepo)

  "createWorkflow" should "work" in {
    val tasks = Seq(
      WorkflowTask(0, "root", "nop", "{}"),
      WorkflowTask(-1, "terminal", "nop", "{}"),
      WorkflowTask(1, "task1", "nop", "{}"),
    )
    val links = Seq((0, 1), (1, -1))

    val wf = wfops.createWorkflow(99, tasks, links)
    println(wf.get.printInfo)
    assert(wf.isSuccess)
    assert(wf.get.id == 99)
    assert(wf.get.tasks == tasks.map(x => (x.id, x)).toMap)

    assert(wf.get.getParents(0) == Seq.empty)
    assert(wf.get.getParents(1) == Seq(0))
    assert(wf.get.getParents(-1) == Seq(1))

    assert(wf.get.getChildren(0) == Seq(1))
    assert(wf.get.getChildren(1) == Seq(-1))
    assert(wf.get.getChildren(-1) == Seq())
  }

  "loadWorkflow" should "work" in {
    
  }
}

