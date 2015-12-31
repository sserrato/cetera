package com.socrata.cetera.util

import scala.util.control.NonFatal

trait ParamConverter[T] {
  def convertFrom(s: String): Either[ParamConversionFailure, T]

  private def convertFrom(s: String, f: (String => T)): Either[ParamConversionFailure, T] =
    try {
      Right(f(s))
    } catch {
      case NonFatal(_) => Left(InvalidValue(s))
    }
}

object ParamConverter {
  implicit object IntParam extends ParamConverter[Int] {
    override def convertFrom(s: String): Either[ParamConversionFailure, Int] = convertFrom(s, _.toInt)
  }

  implicit object FloatParam extends ParamConverter[Float] {
    override def convertFrom(s: String): Either[ParamConversionFailure, Float] = convertFrom(s, _.toFloat)
  }

  def filtered[A: ParamConverter, B](f: A => Option[B]): ParamConverter[B] = {
    new ParamConverter[B] {
      def convertFrom(s: String): Either[ParamConversionFailure, B] = {
        implicitly[ParamConverter[A]].convertFrom(s) match {
          case Right(a) => f(a).map(Right(_)).getOrElse(Left(InvalidValue(s)))
          case Left(e) => Left(e)
        }
      }
    }
  }
}

sealed trait ParamConversionFailure
case class InvalidValue(v: String) extends ParamConversionFailure
