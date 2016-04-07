package com.socrata.cetera.authentication

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

case class User(id: String,
                roleName: Option[String],
                rights: Option[Seq[String]],
                flags: Option[Seq[String]]) {

  def canViewCatalog: Boolean = {
    val isAdmin = flags.exists(_.contains("admin"))
    val hasRole = roleName.nonEmpty

    hasRole || isAdmin
  }
}

object User {
  implicit val jCodec = AutomaticJsonCodecBuilder[User]
}
