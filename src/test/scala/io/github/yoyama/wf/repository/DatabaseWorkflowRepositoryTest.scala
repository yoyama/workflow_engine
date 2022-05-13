package io.github.yoyama.wf.repository
import io.github.yoyama.wf.db.model.running.{WorkflowRunAll, WorkflowRun, TaskRun, LinkRun}
import scalikejdbc.*
import scalikejdbc.config.*
import org.scalatest.flatspec.AnyFlatSpec

import java.time.ZonedDateTime

class DatabaseWorkflowRepositoryTest  extends AnyFlatSpec {
  Class.forName("org.postgresql.Driver")
  //DBs.setup()
  val jdbcUrl = sys.env.getOrElse("TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/test_workflow")
  val jdbcUser = sys.env.getOrElse("TEST_JDBC_USER", "test")
  val jdbcPass = sys.env.getOrElse("TEST_JDBC_PASS", "testtest")
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPass)
  implicit val transactionRunner:TransactionRunner = new ScalikeJDBCTransactionRunner()

  "assignNewRunId" should "work" in {
    val repo = new DatabaseWorkflowRepository()
    val ret = repo.assignNewRunId().run
    val id1 = {
      val ret = repo.assignNewRunId().run
      assert(ret.v.isRight)
      ret.v.getOrElse(-1)
    }
    val id2 = {
      val ret = repo.assignNewRunId().run
      assert(ret.v.isRight)
      ret.v.getOrElse(-1)
    }
    println(id2)
    assert(id2-id1 >= 1)
  }

  "saveNewWorkflowRunAll" should "work" in {
    val now = ZonedDateTime.now()
    val repo = new DatabaseWorkflowRepository()

    val wfa: WorkflowRunAll = WorkflowRunAll(
      wf = WorkflowRun(runId = 1, name = "test1", state = 0, createdAt = now, updatedAt = now),
      tasks = Seq(
        TaskRun(id = 2, runId = 1, name = "t2", `type` ="aaaa", config = "{}", state = 0, createdAt = now, updatedAt = now),
        TaskRun(id = 3, runId = 1, name = "t3", `type` ="aaaa", config = "{}", state = 0, createdAt = now, updatedAt = now)
      ),
      links = Seq(
        //LinkRun()
      )
    )
    val ret = repo.saveNewWorkflowRunAll(wfa).run
    println(ret)
    assert(ret.v.isRight)

  }

  "updateNewWorkflowRunAll" should "work" in {
    val now = ZonedDateTime.now()
    val repo = new DatabaseWorkflowRepository()

    val wfa: WorkflowRunAll = WorkflowRunAll(
      wf = WorkflowRun(runId = 1, name = "test1", state = 0, createdAt = now, updatedAt = now),
      tasks = Seq(
        TaskRun(id = 2, runId = 1, name = "t1", `type` ="aaaa", config = "{}", state = 0, createdAt = now, updatedAt = now)
      ),
      links = Seq()
    )
    val ret = repo.saveNewWorkflowRunAll(wfa).run
    val wfa1 = ret.v.getOrElse(fail("Failed to insert"))
    val wfa2 = wfa1.copy(tasks = wfa1.tasks.map(_.copy(state = 999)))
    println("wfa2:" + wfa2);
    val ret2 = repo.updateWorkflowRunAll(wfa2).run
    println("ret2:" + ret2)
    assert(ret2.v.isRight)
  }
}
