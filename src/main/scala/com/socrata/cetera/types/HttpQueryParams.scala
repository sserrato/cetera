package com.socrata.cetera.types

import com.socrata.http.server.HttpRequest

object HttpQueryParams {
  type MultiQueryParams = Map[String,Seq[String]]

  implicit class HttpRequestCompanion(req: HttpRequest) {
    def multiQueryParams: MultiQueryParams =
      req.allQueryParameters.mapValues(_.flatten)
  }

  /////////////////////////////////////////////////////
  // code from rjmac likely to be added to socrata-http
  //
  sealed trait ParamConversionFailure
  case class InvalidValue(v: String) extends ParamConversionFailure

  trait ParamConverter[T] {
    def convertFrom(s: String): Either[ParamConversionFailure, T]
  }

  // monkeys have been here
  implicit class TypedQueryParams(queryParameters: MultiQueryParams) {
    def getFirst(name: String): Option[String] = {
      queryParameters.get(name).flatMap(_.headOption)
    }

    def getTypedSeq[T: ParamConverter](name: String): Option[Seq[Either[ParamConversionFailure, T]]] = {
      queryParameters.get(name).map {
        _.map(implicitly[ParamConverter[T]].convertFrom)
      }
    }

    def getTypedFirst[T: ParamConverter](name: String): Option[Either[ParamConversionFailure, T]] = {
      queryParameters.getTypedSeq[T](name).flatMap(_.headOption)
    }

    def getTypedFirstOrElse[T: ParamConverter](name: String, default: T): Either[ParamConversionFailure, T] = {
      getTypedFirst[T](name).getOrElse(Right(default))
    }
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
  //
  /////////////////////////////////////////////////////
}
