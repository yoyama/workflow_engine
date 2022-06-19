package io.github.yoyama.wf.db.model.running

import scalikejdbc._
import java.time.{ZonedDateTime}

case class TaskRun(
                    taskId: Int,
                    runId: Int,
                    name: String,
                    `type`: String,
                    config: String,
                    state: Int,
                    inputParams: Option[String] = None,
                    outputParams: Option[String] = None,
                    systemParams: Option[String] = None,
                    stateParams: Option[String] = None,
                    nextPoll: Option[ZonedDateTime] = None,
                    result: Option[Int] = None,
                    errCode: Option[Int] = None,
                    startAt: Option[ZonedDateTime] = None,
                    finishAt: Option[ZonedDateTime] = None,
                    tag: Option[String] = None,
                    createdAt: ZonedDateTime,
                    updatedAt: ZonedDateTime) {

  def save()(implicit session: DBSession): TaskRun = TaskRun.save(this)(session)

  def destroy()(implicit session: DBSession): Int = TaskRun.destroy(this)(session)

}


object TaskRun extends SQLSyntaxSupport[TaskRun] {

  override val schemaName = Some("running")

  override val tableName = "task_run"

  override val columns = Seq("task_id", "run_id", "name", "type", "config", "state", "input_params", "output_params", "system_params", "state_params", "next_poll", "result", "err_code", "start_at", "finish_at", "tag", "created_at", "updated_at")

  def apply(tr: SyntaxProvider[TaskRun])(rs: WrappedResultSet): TaskRun = apply(tr.resultName)(rs)
  def apply(tr: ResultName[TaskRun])(rs: WrappedResultSet): TaskRun = new TaskRun(
    taskId = rs.get(tr.taskId),
    runId = rs.get(tr.runId),
    name = rs.get(tr.name),
    `type` = rs.get(tr.`type`),
    config = rs.get(tr.config),
    state = rs.get(tr.state),
    inputParams = rs.stringOpt(tr.inputParams),
    outputParams = rs.stringOpt(tr.outputParams),
    systemParams = rs.stringOpt(tr.systemParams),
    stateParams = rs.stringOpt(tr.stateParams),
    nextPoll = rs.get(tr.nextPoll),
    result = rs.get(tr.result),
    errCode = rs.get(tr.errCode),
    startAt = rs.get(tr.startAt),
    finishAt = rs.get(tr.finishAt),
    tag = rs.stringOpt(tr.tag),
    createdAt = rs.get(tr.createdAt),
    updatedAt = rs.get(tr.updatedAt)
  )

  val tr = TaskRun.syntax("tr")

  override val autoSession = AutoSession

  def find(taskId: Int, runId: Int)(implicit session: DBSession): Option[TaskRun] = {
    sql"""select ${tr.result.*} from ${TaskRun as tr} where ${tr.taskId} = ${taskId} and ${tr.runId} = ${runId}"""
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

  def create(t:TaskRun)(implicit session: DBSession): TaskRun = {
    create(
      t.taskId, t.runId, t.name, t.`type`, t.config,
      t.state, t.inputParams, t.outputParams, t.systemParams, t.stateParams, t.nextPoll,
      t.result, t.errCode, t.startAt, t.finishAt, t.tag, t.createdAt, t.updatedAt)
  }

  def create(
              taskId: Int,
              runId: Int,
              name: String,
              `type`: String,
              config: String,
              state: Int,
              inputParams: Option[String] = None,
              outputParams: Option[String] = None,
              systemParams: Option[String] = None,
              stateParams: Option[String] = None,
              nextPoll: Option[ZonedDateTime] = None,
              result: Option[Int] = None,
              errCode: Option[Int] = None,
              startAt: Option[ZonedDateTime] = None,
              finishAt: Option[ZonedDateTime] = None,
              tag: Option[String] = None,
              createdAt: ZonedDateTime,
              updatedAt: ZonedDateTime)(implicit session: DBSession): TaskRun = {
    sql"""
      insert into ${TaskRun.table} (
        ${column.taskId},
        ${column.runId},
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
        ${taskId},
        ${runId},
        ${name},
        ${`type`},
        ${config}::jsonb,
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
      taskId = taskId,
      runId = runId,
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
        "taskId" -> entity.taskId,
        "runId" -> entity.runId,
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
      taskId,
      run_id,
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
      {runId},
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
        ${column.taskId} = ${entity.taskId},
        ${column.runId} = ${entity.runId},
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
        ${column.taskId} = ${entity.taskId} and ${column.runId} = ${entity.runId}
      """.update.apply()
    entity
  }

  def destroy(entity: TaskRun)(implicit session: DBSession): Int = {
    sql"""delete from ${TaskRun.table} where ${column.taskId} = ${entity.taskId} and ${column.runId} = ${entity.runId}""".update.apply()
  }
}
