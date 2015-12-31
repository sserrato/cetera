package com.socrata.cetera.util

trait ParamConverter[T] {
  def convertFrom(s: String): Either[ParamConversionFailure, T]
}

object ParamConverter {
  implicit object IntParam extends ParamConverter[Int] {
    override def convertFrom(s: String): Either[ParamConversionFailure, Int] = {
      try { Right(s.toInt) }
      catch { case e: Exception => Left(InvalidValue(s)) }
    }
  }

  implicit object FloatParam extends ParamConverter[Float] {
    override def convertFrom(s: String): Either[ParamConversionFailure, Float] = {
      try { Right(s.toFloat) }
      catch { case e: Exception => Left(InvalidValue(s)) }
    }
  }

  def filtered[A: ParamConverter, B](f: A => Option[B]): ParamConverter[B] = {
    new ParamConverter[B] {
      def convertFrom(s: String): Either[ParamConversionFailure, B] = {
        implicitly[ParamConverter[A]].convertFrom(s) match {
          case Right(a) =>
            f(a) match {
              case Some(b) => Right(b)
              case None => Left(InvalidValue(s))
            }
          case Left(e) => Left(e)
        }
      }
    }
  }
}

sealed trait ParamConversionFailure
case class InvalidValue(v: String) extends ParamConversionFailure
