package io.github.yoyama.wf.messaging

// From Task to Workflow

/**
 * 
 * @param id message unique id
 * @param workflowId id of the workflow the source
 * @param taskId id of the task of the source
 * @param mType message type  (1:task started 2:task running 3:task stopped successfully 4 task stopped with failures)
 * @param body message body(JSON)
 */
case class WorkflowMessage(id:Int, workflowId:Int, taskId:Int, mType:Int, body:String)
