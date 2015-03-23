package com.socrata.cetera.util

import com.socrata.http.server.HttpRequest

case class ValidatedQueryParameters(
  searchQuery: Option[String],
  domains: Option[Set[String]],
  categories: Option[Set[String]],
  tags: Option[Set[String]],
  only: Option[String],
  offset: Int,
  limit: Int
)

sealed trait ParseError { def message: String }

case class OnlyError(override val message: String) extends ParseError
case class LimitError(override val message: String) extends ParseError

// Parses and validates
object QueryParametersParser {

  /////////////////////////////////////////////////////
  // code from rjmac likely to be added to socrata-http
  //
  sealed trait ParamConversionFailure
  case class InvalidValue(v: String) extends ParamConversionFailure

  trait ParamConverter[T] {
    def convertFrom(s: String): Either[ParamConversionFailure, T]
  }

  implicit class TypedQueryParams(req: HttpRequest) {
    def queryParamOrElse[T: ParamConverter](name: String, default: T): Either[ParamConversionFailure, T] = {
      req.queryParameters
        .get(name)
        .map(implicitly[ParamConverter[T]].convertFrom)
        .getOrElse(Right(default))
    }
  }

  def validated[T](x: Either[ParamConversionFailure, T]): T = x match {
    case Right(v) => v
    case Left(_) => throw new Exception("Parameter validation failure")
  }

  object ParamConverter {
    implicit object IntParam extends ParamConverter[Int] {
      override def convertFrom(s: String): Either[ParamConversionFailure, Int] = {
        try { Right(s.toInt) }
        catch { case e : Exception => Left(InvalidValue(s)) }
      }
    }

    def filtered[A : ParamConverter, B](f: A => Option[B]): ParamConverter[B] = {
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


  case class NonNegativeInt(value: Int)
  implicit val nonNegativeIntParamConverter = ParamConverter.filtered { (a: Int) =>
    if (a >= 0)
      Some(NonNegativeInt(a))
    else
      None
  }

  def apply(req: HttpRequest): Either[Seq[ParseError], ValidatedQueryParameters] = {
    val searchQuery = req.queryParameters.get("q")
    val domains = req.queryParameters.get("domains").map(_.split(",").toSet)
    val categories = req.queryParameters.get("categories").map(_.split(",").toSet)
    val tags = req.queryParameters.get("tags").map(_.split(",").toSet)

    val only = req.queryParameters.get("only") match {
      case Some("datasets") => Right(Some("dataset"))
      case Some("pages") => Right(Some("page"))
      case Some(invalid) =>
        Left(OnlyError(s"only must be one of {datasets, pages} got only=${invalid}"))
      case None => Right(None)
    }

    val offset = validated(req.queryParamOrElse("offset", NonNegativeInt(0))).value
    val limit = validated(req.queryParamOrElse("limit", NonNegativeInt(100))).value

    only match {
      case Right(o) =>
        Right(ValidatedQueryParameters(
          searchQuery,
          domains,
          categories,
          tags,
          o,
          offset,
          limit
        ))

      case Left(e) => Left(Seq(e))
    }
  }
}
