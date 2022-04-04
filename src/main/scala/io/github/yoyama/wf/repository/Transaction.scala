package io.github.yoyama.wf.repository

import scala.util.{Try,Success,Failure}

case class TransactionResult[A](v:Either[Throwable,A]) {}

trait Transaction[A] { self =>
  def map[B](f: A => B): Transaction[B]
  def flatMap[B](f: A => Transaction[B]): Transaction[B]
  def run(implicit runner: TransactionRunner): TransactionResult[A] = {
    runner.run(self)
  }
}

trait TransactionRunner {
  def run[A](transaction: Transaction[A]): TransactionResult[A]
}

import scalikejdbc._

case class ScalikeJDBCTransaction[A](execute: DBSession => Either[Throwable,A]) extends Transaction[A] {

  override def map[B](f: A => B): ScalikeJDBCTransaction[B] = {
    val exec = (session: DBSession) => execute(session).map(f)

    ScalikeJDBCTransaction(exec)
  }

  override def flatMap[B](f: A => Transaction[B]): Transaction[B] = {
    val exec = (session: DBSession) => execute(session).map(f).flatMap(_.asInstanceOf[ScalikeJDBCTransaction[B]].execute(session))
    ScalikeJDBCTransaction(exec)
  }
}

object ScalikeJDBCTransaction {
  def from[A](execute: DBSession => A): ScalikeJDBCTransaction[A] = {
    val exec = (session: DBSession) => {
      Try {
        execute(session)
      } match {
        case Success(r) => Right(r)
        case Failure(l) => Left(l)
      }
    }
    ScalikeJDBCTransaction(exec)
  }
}

class ScalikeJDBCTransactionRunner extends TransactionRunner {
  override def run[A](transaction: Transaction[A]): TransactionResult[A] = {
    TransactionResult {
      DB localTx { session =>
        transaction.asInstanceOf[ScalikeJDBCTransaction[A]].execute(session)
      }
    }
  }
}