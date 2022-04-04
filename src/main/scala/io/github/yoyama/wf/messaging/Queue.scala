package io.github.yoyama.wf.messaging

import scala.util.Try

trait Queue[T] {
  def send(m:T):Try[Unit]
  def receive(num:Int): Try[Seq[T]]
}

class TaskQueue extends Queue[TaskMessage] {
  def send(m:TaskMessage):Try[Unit] = ???
  def receive(num:Int): Try[Seq[TaskMessage]] = ???
}
class WorkflowQueue extends Queue[WorkflowMessage] {
  def send(m:WorkflowMessage):Try[Unit] = ???
  def receive(num:Int): Try[Seq[WorkflowMessage]] = ???

}