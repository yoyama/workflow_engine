package io.github.yoyama.wf.workflow

import io.github.yoyama.wf.db.model.running.{LinkRun, TaskRun, WorkflowRun, WorkflowRunAll}
import io.github.yoyama.wf.repository.*
import io.github.yoyama.wf.tag.Tag
import org.scalatest.flatspec.AnyFlatSpec
import scalikejdbc.ConnectionPool

import scala.util.{Failure, Success, Try}

class WorkflowDagBuilderTest extends AnyFlatSpec {
  val jdbcUrl = sys.env.getOrElse("TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/test_workflow")
  val jdbcUser = sys.env.getOrElse("TEST_JDBC_USER", "test")
  val jdbcPass = sys.env.getOrElse("TEST_JDBC_PASS", "testtest")
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPass)

  implicit val tRunner:TransactionRunner = new ScalikeJDBCTransactionRunner()
  val wfRepo = new DatabaseWorkflowRunRepository
  val wfBuilder = new WorkflowDagBuilder(wfRepo)

  val testTasks = Seq(
    WorkflowTask(0, 99, "root", "nop", "{}", state = TaskState.READY, createdAt = null, updatedAt = null),
    WorkflowTask(-1, 99, "terminal", "nop", "{}", state = TaskState.WAIT, createdAt = null, updatedAt = null),
    WorkflowTask(1, 99, "task1", "nop", "{}", state = TaskState.WAIT, createdAt = null, updatedAt = null),
    WorkflowTask(2, 99, "task2", "nop", "{}", state = TaskState.WAIT, createdAt = null, updatedAt = null),
    WorkflowTask(3, 99, "task3", "nop", "{}", state = TaskState.WAIT, createdAt = null, updatedAt = null),
  )
  val testLinks = Seq((0, 1), (0, 2), (1, 3), (2, 3), (3, -1))
  val testTag = Tag.from("""{ "type" : "normal" } """).get

  "buildWorkflowDag from WorkflowTask" should "work" in {
    val wf = wfBuilder.buildWorkflowDag(99, "wf1", testTasks, testLinks, tags = testTag)
    println(wf.get.printInfo)
    assert(wf.isSuccess)
    assert(wf.get.id == 99)
    assert(wf.get.tasks == testTasks.map(x => (x.id, x)).toMap)

    assert(wf.get.getParents(0) == Seq.empty)
    assert(wf.get.getParents(1) == Seq(0))
    assert(wf.get.getParents(-1) == Seq(3))

    assert(wf.get.getChildren(0) == Seq(1,2))
    assert(wf.get.getChildren(1) == Seq(3))
    assert(wf.get.getChildren(2) == Seq(3))
    assert(wf.get.getChildren(3) == Seq(-1))
    assert(wf.get.getChildren(-1) == Seq())
  }

  "loadWorkflow" should "work" in {
    val wfa = WorkflowRunAll(
      wf = WorkflowRun(runId = 4, name = "wf_001", state = 0),
      tasks = Seq(
        TaskRun(taskId = 0, runId = 4, name = "root", `type` = "nop", config = "{}", state = 0),
        TaskRun(taskId = -1, runId = 4, name = "terminal", `type` = "nop", config = "{}", state = 0),
        TaskRun(taskId = 1, runId = 4, name = "task_001", `type` = "nop", config = "{}", state = 0),
        TaskRun(taskId = 2, runId = 4, name = "task_002", `type` = "nop", config = "{}", state = 0),
        TaskRun(taskId = 3, runId = 4, name = "task_003", `type` = "nop", config = "{}", state = 0)),
      links = Seq(
        LinkRun(runId = 4, parent = 0, child = 1),
        LinkRun(runId = 4, parent = 0, child = 2),
        LinkRun(runId = 4, parent = 1, child = 3),
        LinkRun(runId = 4, parent = 2, child = 3),
        LinkRun(runId = 4, parent = 3, child = -1)
      )
    )
    val ret = for {
      savedWf <- wfRepo.saveNewWorkflowRunAll(wfa).run.v.toTry
      loadWf <- wfBuilder.loadWorkflowDag(savedWf.wf.runId)
    } yield loadWf
    ret match {
      case Failure(e) => fail(e)
      case Success(v) => {
        assert(v.getChildren(0).toSet == Set(1,2))
        assert(v.getChildren(1).toSet == Set(3))
        assert(v.getChildren(2).toSet == Set(3))
        assert(v.getChildren(3).toSet == Set(-1))
      }
    }
  }
}

