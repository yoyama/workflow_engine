package io.github.yoyama.wf.db.model.running

import scalikejdbc._
import java.time.{ZonedDateTime}

case class WorkflowRun(
  id: Int,
  name: String,
  state: Int,
  startAt: Option[ZonedDateTime] = None,
  finishAt: Option[ZonedDateTime] = None,
  tag: Option[Any] = None,
  createdAt: ZonedDateTime,
  updatedAt: ZonedDateTime) {

  def save()(implicit session: DBSession): WorkflowRun = WorkflowRun.save(this)(session)

  def destroy()(implicit session: DBSession): Int = WorkflowRun.destroy(this)(session)

}


object WorkflowRun extends SQLSyntaxSupport[WorkflowRun] {

  override val schemaName = Some("running")

  override val tableName = "workflow_run"

  override val columns = Seq("id", "name", "state", "start_at", "finish_at", "tag", "created_at", "updated_at")

  def apply(wr: SyntaxProvider[WorkflowRun])(rs: WrappedResultSet): WorkflowRun = apply(wr.resultName)(rs)
  def apply(wr: ResultName[WorkflowRun])(rs: WrappedResultSet): WorkflowRun = new WorkflowRun(
    id = rs.get(wr.id),
    name = rs.get(wr.name),
    state = rs.get(wr.state),
    startAt = rs.get(wr.startAt),
    finishAt = rs.get(wr.finishAt),
    tag = rs.anyOpt(wr.tag),
    createdAt = rs.get(wr.createdAt),
    updatedAt = rs.get(wr.updatedAt)
  )

  val wr = WorkflowRun.syntax("wr")

  override val autoSession = AutoSession

  def find(id: Int)(implicit session: DBSession): Option[WorkflowRun] = {
    sql"""select ${wr.result.*} from ${WorkflowRun as wr} where ${wr.id} = ${id}"""
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
    create(w.id, w.name, w.state, w.startAt, w.finishAt, w.tag, w.createdAt, w.updatedAt)(session)
  }
  
  def create(
    id: Int,
    name: String,
    state: Int,
    startAt: Option[ZonedDateTime] = None,
    finishAt: Option[ZonedDateTime] = None,
    tag: Option[Any] = None,
    createdAt: ZonedDateTime,
    updatedAt: ZonedDateTime)(implicit session: DBSession): WorkflowRun = {
    sql"""
      insert into ${WorkflowRun.table} (
        ${column.id},
        ${column.name},
        ${column.state},
        ${column.startAt},
        ${column.finishAt},
        ${column.tag},
        ${column.createdAt},
        ${column.updatedAt}
      ) values (
        ${id},
        ${name},
        ${state},
        ${startAt},
        ${finishAt},
        ${tag},
        ${createdAt},
        ${updatedAt}
      )
      """.update.apply()

    WorkflowRun(
      id = id,
      name = name,
      state = state,
      startAt = startAt,
      finishAt = finishAt,
      tag = tag,
      createdAt = createdAt,
      updatedAt = updatedAt)
  }

  def batchInsert(entities: collection.Seq[WorkflowRun])(implicit session: DBSession): List[Int] = {
    val params: collection.Seq[Seq[(String, Any)]] = entities.map(entity =>
      Seq(
        "id" -> entity.id,
        "name" -> entity.name,
        "state" -> entity.state,
        "startAt" -> entity.startAt,
        "finishAt" -> entity.finishAt,
        "tag" -> entity.tag,
        "createdAt" -> entity.createdAt,
        "updatedAt" -> entity.updatedAt))
    SQL("""insert into workflow_run(
      id,
      name,
      state,
      start_at,
      finish_at,
      tag,
      created_at,
      updated_at
    ) values (
      {id},
      {name},
      {state},
      {startAt},
      {finishAt},
      {tag},
      {createdAt},
      {updatedAt}
    )""").batchByName(params.toSeq: _*).apply[List]()
  }

  def save(entity: WorkflowRun)(implicit session: DBSession): WorkflowRun = {
    sql"""
      update
        ${WorkflowRun.table}
      set
        ${column.id} = ${entity.id},
        ${column.name} = ${entity.name},
        ${column.state} = ${entity.state},
        ${column.startAt} = ${entity.startAt},
        ${column.finishAt} = ${entity.finishAt},
        ${column.tag} = ${entity.tag},
        ${column.createdAt} = ${entity.createdAt},
        ${column.updatedAt} = ${entity.updatedAt}
      where
        ${column.id} = ${entity.id}
      """.update.apply()
    entity
  }

  def destroy(entity: WorkflowRun)(implicit session: DBSession): Int = {
    sql"""delete from ${WorkflowRun.table} where ${column.id} = ${entity.id}""".update.apply()
  }

}
