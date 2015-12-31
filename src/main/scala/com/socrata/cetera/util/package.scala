package com.socrata.cetera

import com.socrata.http.server.HttpRequest

package object util {
  type MultiQueryParams = Map[String,Seq[String]]

  implicit class HttpRequestCompanion(req: HttpRequest) {
    def multiQueryParams: MultiQueryParams =
      req.allQueryParameters.mapValues(_.flatten)
  }

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
}
