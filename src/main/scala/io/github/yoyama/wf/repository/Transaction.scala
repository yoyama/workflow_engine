package io.github.yoyama.wf.repository

import cats.Applicative
import cats.Monad

import scala.util.{Failure, Success, Try}

case class TransactionResult[A](v:Either[Throwable,A]) {}


trait Transaction[A] { self =>
  def pure[A](x:A): Transaction[A]
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
  override def pure[A](x: A): Transaction[A] = {
    ScalikeJDBCTransaction.from(x)
  }

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
  //import cats.Applicative
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

  def from[A](a:A): ScalikeJDBCTransaction[A] = {
    ScalikeJDBCTransaction.from{ (s) => a}
  }

  implicit val apl: Applicative[ScalikeJDBCTransaction] = new Applicative[ScalikeJDBCTransaction] {
    override def pure[A](x: A): ScalikeJDBCTransaction[A] = {
      ScalikeJDBCTransaction.from(x)
    }

    override def ap[A, B](ff: ScalikeJDBCTransaction[A => B])(fa: ScalikeJDBCTransaction[A]): ScalikeJDBCTransaction[B] = {
      val ret = for {
        a <- fa
        f <- ff
      } yield f(a)
      ret.asInstanceOf[ScalikeJDBCTransaction[B]]
    }
  }

  /**
  implicit val monad: Monad[ScalikeJDBCTransaction] = new Monad[ScalikeJDBCTransaction] {
    override def pure[A](x: A): ScalikeJDBCTransaction[A] = {
      ScalikeJDBCTransaction.from(x)
    }
    override def flatMap[A, B](fa: ScalikeJDBCTransaction[A])(f: A => ScalikeJDBCTransaction[B]): ScalikeJDBCTransaction[B] = {
      fa.flatMap(f).asInstanceOf[ScalikeJDBCTransaction[B]]
    }

    override def tailRecM[A, B](a: A)(f: A => ScalikeJDBCTransaction[Either[A, B]]): ScalikeJDBCTransaction[B] = {
      ???
    }
  }
  */
}

class ScalikeJDBCTransactionRunner extends TransactionRunner {
  override def run[A](transaction: Transaction[A]): TransactionResult[A] = {
    TransactionResult {
      DB localTx { session =>
        transaction.asInstanceOf[ScalikeJDBCTransaction[A]].execute(session)
      } match {
        case v if (v.isRight) => v
        case v => {
          v match {
            case Left(l) => l.printStackTrace()
            case Right(r) =>
          }
          v
        }
      }
    }
  }
}