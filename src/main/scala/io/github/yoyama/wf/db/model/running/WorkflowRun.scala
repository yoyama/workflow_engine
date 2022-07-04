package io.github.yoyama.wf.db.model.running

import scalikejdbc._

import java.time.Instant

case class WorkflowRun(
  runId: Int,
  name: String,
  state: Int,
  startAt: Option[Instant] = None,
  finishAt: Option[Instant] = None,
  tags: Option[String] = None,
  createdAt: Instant,
  updatedAt: Instant) {

  def save()(implicit session: DBSession): WorkflowRun = WorkflowRun.save(this)(session)

  def destroy()(implicit session: DBSession): Int = WorkflowRun.destroy(this)(session)

}


object WorkflowRun extends SQLSyntaxSupport[WorkflowRun] {

  override val schemaName = Some("running")

  override val tableName = "workflow_run"

  override val columns = Seq("run_id", "name", "state", "start_at", "finish_at", "tags", "created_at", "updated_at")

  def apply(wr: SyntaxProvider[WorkflowRun])(rs: WrappedResultSet): WorkflowRun = apply(wr.resultName)(rs)
  def apply(wr: ResultName[WorkflowRun])(rs: WrappedResultSet): WorkflowRun = new WorkflowRun(
    runId = rs.get(wr.runId),
    name = rs.get(wr.name),
    state = rs.get(wr.state),
    startAt = rs.get(wr.startAt),
    finishAt = rs.get(wr.finishAt),
    tags = rs.stringOpt(wr.tags),
    createdAt = rs.get(wr.createdAt),
    updatedAt = rs.get(wr.updatedAt)
  )

  val wr = WorkflowRun.syntax("wr")

  override val autoSession = AutoSession

  def find(runId: Int)(implicit session: DBSession): Option[WorkflowRun] = {
    sql"""select ${wr.result.*} from ${WorkflowRun as wr} where ${wr.runId} = ${runId}"""
      .map(WorkflowRun(wr.resultName)).single.apply()
  }

  def findAll()(implicit session: DBSession): List[WorkflowRun] = {
    sql"""select ${wr.result.*} from ${WorkflowRun as wr}""".map(WorkflowRun(wr.resultName)).list.apply()
  }

  def countAll()(implicit session: DBSession): Long = {
    sql"""select count(1) from ${WorkflowRun.table}""".map(rs => rs.long(1)).single.apply().get
  }

  def findBy(where: SQLSyntax)(implicit session: DBSession): Option[WorkflowRun] = {
    sql"""select ${wr.result.*} from ${WorkflowRun as wr} where ${where}"""
      .map(WorkflowRun(wr.resultName)).single.apply()
  }

  def findAllBy(where: SQLSyntax)(implicit session: DBSession): List[WorkflowRun] = {
    sql"""select ${wr.result.*} from ${WorkflowRun as wr} where ${where}"""
      .map(WorkflowRun(wr.resultName)).list.apply()
  }

  def countBy(where: SQLSyntax)(implicit session: DBSession): Long = {
    sql"""select count(1) from ${WorkflowRun as wr} where ${where}"""
      .map(_.long(1)).single.apply().get
  }

  def create(w:WorkflowRun)(implicit session: DBSession): WorkflowRun = {
    create(w.runId, w.name, w.state, w.startAt, w.finishAt, w.tags)(session)
  }

  def create(
    runId: Int,
    name: String,
    state: Int,
    startAt: Option[Instant] = None,
    finishAt: Option[Instant] = None,
    tags: Option[String] = None)(implicit session: DBSession): WorkflowRun = {
    println(s"YY tags: ${tags}")
    sql"""
      insert into ${WorkflowRun.table} (
        ${column.runId},
        ${column.name},
        ${column.state},
        ${column.startAt},
        ${column.finishAt},
        ${column.tags},
        ${column.createdAt},
        ${column.updatedAt}
      ) values (
        ${runId},
        ${name},
        ${state},
        ${startAt},
        ${finishAt},
        jsonb(${tags.getOrElse("{}")}),
        now(),
        now()
      )
      """.update.apply()

    find(runId).get
  }

  def batchInsert(entities: collection.Seq[WorkflowRun])(implicit session: DBSession): List[Int] = {
    val params: collection.Seq[Seq[(String, Any)]] = entities.map(entity =>
      Seq(
        "runId" -> entity.runId,
        "name" -> entity.name,
        "state" -> entity.state,
        "startAt" -> entity.startAt,
        "finishAt" -> entity.finishAt,
        "tags" -> entity.tags,
        "createdAt" -> entity.createdAt,
        "updatedAt" -> entity.updatedAt))
    SQL("""insert into workflow_run(
      run_id,
      name,
      state,
      start_at,
      finish_at,
      tags,
      created_at,
      updated_at
    ) values (
      {runId},
      {name},
      {state},
      {startAt},
      {finishAt},
      jsonb({tags}),
      {createdAt},
      {updatedAt}
    )""").batchByName(params.toSeq: _*).apply[List]()
  }

  def save(entity: WorkflowRun)(implicit session: DBSession): WorkflowRun = {
    sql"""
      update
        ${WorkflowRun.table}
      set
        ${column.runId} = ${entity.runId},
        ${column.name} = ${entity.name},
        ${column.state} = ${entity.state},
        ${column.startAt} = ${entity.startAt},
        ${column.finishAt} = ${entity.finishAt},
        ${column.tags} = jsonb(${entity.tags}),
        ${column.createdAt} = ${entity.createdAt},
        ${column.updatedAt} = ${entity.updatedAt}
      where
        ${column.runId} = ${entity.runId}
      """.update.apply()
    entity
  }

  def destroy(entity: WorkflowRun)(implicit session: DBSession): Int = {
    sql"""delete from ${WorkflowRun.table} where ${column.runId} = ${entity.runId}""".update.apply()
  }

}
