package io.github.yoyama.wf.messaging

// From Workflow to Task
case class TaskMessage(id:Int, workflowId:Int, taskId:Int, mType:Int, body:String)
