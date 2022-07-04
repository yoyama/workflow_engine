package io.github.yoyama.wf.workflow

import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun}
import io.github.yoyama.wf.repository.{DatabaseWorkflowRunRepository, ScalikeJDBCTransactionRunner, Transaction, TransactionResult, TransactionRunner, WorkflowRunRepository}
import io.github.yoyama.wf.tag.Tag
import org.scalatest.flatspec.AnyFlatSpec
import scalikejdbc.ConnectionPool

class WorkflowDagOpsTest extends AnyFlatSpec {
  val jdbcUrl = sys.env.getOrElse("TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/test_workflow")
  val jdbcUser = sys.env.getOrElse("TEST_JDBC_USER", "test")
  val jdbcPass = sys.env.getOrElse("TEST_JDBC_PASS", "testtest")
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPass)

  implicit val tRunner:TransactionRunner = new ScalikeJDBCTransactionRunner()
  val wfRepo = new DatabaseWorkflowRunRepository
  val wfops = new WorkflowDagOps(wfRepo)

  "createWorkflow" should "work" in {
    val tasks = Seq(
      WorkflowTask(0, 99, "root", "nop", "{}", createdAt = null, updatedAt = null),
      WorkflowTask(-1, 99, "terminal", "nop", "{}", createdAt = null, updatedAt = null),
      WorkflowTask(1, 99, "task1", "nop", "{}", createdAt = null, updatedAt = null),
    )
    val links = Seq((0, 1), (1, -1))

    val wf = wfops.buildWorkflowDag(99, "wf1", tasks, links, tags = Tag())
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

  "saveNewWorkflow" should "work" in {
    val tasks = Seq(
      WorkflowTask(0, 99, "root", "nop", "{}", createdAt = null, updatedAt = null),
      WorkflowTask(-1, 99, "terminal", "nop", "{}", createdAt = null, updatedAt = null),
      WorkflowTask(1, 99, "task1", "nop", "{}", createdAt = null, updatedAt = null),
    )
    val links = Seq((0, 1), (1, -1))
    val ret = for {
      wfDag1 <- wfops.buildWorkflowDag(99, "wf1", tasks, links, tags = Tag())
      wfDag2 <- wfops.saveNewWorkflowDag(wfDag1)
    } yield wfDag2
    println(ret.get.printInfo)
  }

  "loadWorkflow" should "work" in {
    
  }
}

