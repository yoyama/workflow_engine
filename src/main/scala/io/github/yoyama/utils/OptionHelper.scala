package io.github.yoyama.utils

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object OptionHelper {
  extension[T] (t: Option[T]) {
    def toFuture(message: String): Future[T] = toFuture(new Throwable(message))

    def toFuture(e: Throwable): Future[T] = t match {
      case None => Future.failed(e)
      case Some(v) => Future.successful(v)
    }

    def toTry(message: String): Try[T] = toTry(new Throwable(message))

    def toTry(e: Throwable): Try[T] = t match {
      case None => Failure(e)
      case Some(v) => Success(v)
    }
  }
}
