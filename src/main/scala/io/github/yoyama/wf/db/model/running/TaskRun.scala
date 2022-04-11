package io.github.yoyama.wf.db.model.running

import scalikejdbc._
import java.time.{ZonedDateTime}

case class TaskRun(
  id: Int,
  wfid: Int,
  name: String,
  `type`: String,
  config: Any,
  state: Int,
  inputParams: Option[Any] = None,
  outputParams: Option[Any] = None,
  systemParams: Option[Any] = None,
  stateParams: Option[Any] = None,
  nextPoll: Option[ZonedDateTime] = None,
  result: Option[Int] = None,
  errCode: Option[Int] = None,
  startAt: Option[ZonedDateTime] = None,
  finishAt: Option[ZonedDateTime] = None,
  tag: Option[Any] = None,
  createdAt: ZonedDateTime,
  updatedAt: ZonedDateTime) {

  def save()(implicit session: DBSession): TaskRun = TaskRun.save(this)(session)

  def destroy()(implicit session: DBSession): Int = TaskRun.destroy(this)(session)

}


object TaskRun extends SQLSyntaxSupport[TaskRun] {

  override val schemaName = Some("running")

  override val tableName = "task_run"

  override val columns = Seq("id", "wfid", "name", "type", "config", "state", "input_params", "output_params", "system_params", "state_params", "next_poll", "result", "err_code", "start_at", "finish_at", "tag", "created_at", "updated_at")

  def apply(tr: SyntaxProvider[TaskRun])(rs: WrappedResultSet): TaskRun = apply(tr.resultName)(rs)
  def apply(tr: ResultName[TaskRun])(rs: WrappedResultSet): TaskRun = new TaskRun(
    id = rs.get(tr.id),
    wfid = rs.get(tr.wfid),
    name = rs.get(tr.name),
    `type` = rs.get(tr.`type`),
    config = rs.any(tr.config),
    state = rs.get(tr.state),
    inputParams = rs.anyOpt(tr.inputParams),
    outputParams = rs.anyOpt(tr.outputParams),
    systemParams = rs.anyOpt(tr.systemParams),
    stateParams = rs.anyOpt(tr.stateParams),
    nextPoll = rs.get(tr.nextPoll),
    result = rs.get(tr.result),
    errCode = rs.get(tr.errCode),
    startAt = rs.get(tr.startAt),
    finishAt = rs.get(tr.finishAt),
    tag = rs.anyOpt(tr.tag),
    createdAt = rs.get(tr.createdAt),
    updatedAt = rs.get(tr.updatedAt)
  )

  val tr = TaskRun.syntax("tr")

  override val autoSession = AutoSession

  def find(id: Int, wfid: Int)(implicit session: DBSession): Option[TaskRun] = {
    sql"""select ${tr.result.*} from ${TaskRun as tr} where ${tr.id} = ${id} and ${tr.wfid} = ${wfid}"""
      .map(TaskRun(tr.resultName)).single.apply()
  }

  def findAll()(implicit session: DBSession): List[TaskRun] = {
    sql"""select ${tr.result.*} from ${TaskRun as tr}""".map(TaskRun(tr.resultName)).list.apply()
  }

  def countAll()(implicit session: DBSession): Long = {
    sql"""select count(1) from ${TaskRun.table}""".map(rs => rs.long(1)).single.apply().get
  }

  def findBy(where: SQLSyntax)(implicit session: DBSession): Option[TaskRun] = {
    sql"""select ${tr.result.*} from ${TaskRun as tr} where ${where}"""
      .map(TaskRun(tr.resultName)).single.apply()
  }

  def findAllBy(where: SQLSyntax)(implicit session: DBSession): List[TaskRun] = {
    sql"""select ${tr.result.*} from ${TaskRun as tr} where ${where}"""
      .map(TaskRun(tr.resultName)).list.apply()
  }

  def countBy(where: SQLSyntax)(implicit session: DBSession): Long = {
    sql"""select count(1) from ${TaskRun as tr} where ${where}"""
      .map(_.long(1)).single.apply().get
  }

  def create(
    id: Int,
    wfid: Int,
    name: String,
    `type`: String,
    config: Any,
    state: Int,
    inputParams: Option[Any] = None,
    outputParams: Option[Any] = None,
    systemParams: Option[Any] = None,
    stateParams: Option[Any] = None,
    nextPoll: Option[ZonedDateTime] = None,
    result: Option[Int] = None,
    errCode: Option[Int] = None,
    startAt: Option[ZonedDateTime] = None,
    finishAt: Option[ZonedDateTime] = None,
    tag: Option[Any] = None,
    createdAt: ZonedDateTime,
    updatedAt: ZonedDateTime)(implicit session: DBSession): TaskRun = {
    sql"""
      insert into ${TaskRun.table} (
        ${column.id},
        ${column.wfid},
        ${column.name},
        ${column.`type`},
        ${column.config},
        ${column.state},
        ${column.inputParams},
        ${column.outputParams},
        ${column.systemParams},
        ${column.stateParams},
        ${column.nextPoll},
        ${column.result},
        ${column.errCode},
        ${column.startAt},
        ${column.finishAt},
        ${column.tag},
        ${column.createdAt},
        ${column.updatedAt}
      ) values (
        ${id},
        ${wfid},
        ${name},
        ${`type`},
        ${config},
        ${state},
        ${inputParams},
        ${outputParams},
        ${systemParams},
        ${stateParams},
        ${nextPoll},
        ${result},
        ${errCode},
        ${startAt},
        ${finishAt},
        ${tag},
        ${createdAt},
        ${updatedAt}
      )
      """.update.apply()

    TaskRun(
      id = id,
      wfid = wfid,
      name = name,
      `type` = `type`,
      config = config,
      state = state,
      inputParams = inputParams,
      outputParams = outputParams,
      systemParams = systemParams,
      stateParams = stateParams,
      nextPoll = nextPoll,
      result = result,
      errCode = errCode,
      startAt = startAt,
      finishAt = finishAt,
      tag = tag,
      createdAt = createdAt,
      updatedAt = updatedAt)
  }

  def batchInsert(entities: collection.Seq[TaskRun])(implicit session: DBSession): List[Int] = {
    val params: collection.Seq[Seq[(String, Any)]] = entities.map(entity =>
      Seq(
        "id" -> entity.id,
        "wfid" -> entity.wfid,
        "name" -> entity.name,
        "type" -> entity.`type`,
        "config" -> entity.config,
        "state" -> entity.state,
        "inputParams" -> entity.inputParams,
        "outputParams" -> entity.outputParams,
        "systemParams" -> entity.systemParams,
        "stateParams" -> entity.stateParams,
        "nextPoll" -> entity.nextPoll,
        "result" -> entity.result,
        "errCode" -> entity.errCode,
        "startAt" -> entity.startAt,
        "finishAt" -> entity.finishAt,
        "tag" -> entity.tag,
        "createdAt" -> entity.createdAt,
        "updatedAt" -> entity.updatedAt))
    SQL("""insert into task_run(
      id,
      wfid,
      name,
      type,
      config,
      state,
      input_params,
      output_params,
      system_params,
      state_params,
      next_poll,
      result,
      err_code,
      start_at,
      finish_at,
      tag,
      created_at,
      updated_at
    ) values (
      {id},
      {wfid},
      {name},
      {type},
      {config},
      {state},
      {inputParams},
      {outputParams},
      {systemParams},
      {stateParams},
      {nextPoll},
      {result},
      {errCode},
      {startAt},
      {finishAt},
      {tag},
      {createdAt},
      {updatedAt}
    )""").batchByName(params.toSeq: _*).apply[List]()
  }

  def save(entity: TaskRun)(implicit session: DBSession): TaskRun = {
    sql"""
      update
        ${TaskRun.table}
      set
        ${column.id} = ${entity.id},
        ${column.wfid} = ${entity.wfid},
        ${column.name} = ${entity.name},
        ${column.`type`} = ${entity.`type`},
        ${column.config} = ${entity.config},
        ${column.state} = ${entity.state},
        ${column.inputParams} = ${entity.inputParams},
        ${column.outputParams} = ${entity.outputParams},
        ${column.systemParams} = ${entity.systemParams},
        ${column.stateParams} = ${entity.stateParams},
        ${column.nextPoll} = ${entity.nextPoll},
        ${column.result} = ${entity.result},
        ${column.errCode} = ${entity.errCode},
        ${column.startAt} = ${entity.startAt},
        ${column.finishAt} = ${entity.finishAt},
        ${column.tag} = ${entity.tag},
        ${column.createdAt} = ${entity.createdAt},
        ${column.updatedAt} = ${entity.updatedAt}
      where
        ${column.id} = ${entity.id} and ${column.wfid} = ${entity.wfid}
      """.update.apply()
    entity
  }

  def destroy(entity: TaskRun)(implicit session: DBSession): Int = {
    sql"""delete from ${TaskRun.table} where ${column.id} = ${entity.id} and ${column.wfid} = ${entity.wfid}""".update.apply()
  }

}