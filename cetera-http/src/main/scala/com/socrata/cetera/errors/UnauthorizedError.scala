package com.socrata.cetera.errors

import com.socrata.cetera.auth.User

case class UnauthorizedError(user: Option[User], action: String) extends Throwable {
  override def getMessage: String = {
    user match {
      case Some(u) => s"User ${u.id} is not authorized to $action"
      case None => s"No user was provided to $action"
    }
  }
}
