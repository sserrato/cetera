package com.socrata.cetera.authentication

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

case class User(id: String,
                email: String,
                roleName: Option[String],
                rights: Option[Seq[String]],
                flags: Option[Seq[String]]) {

  def canViewCatalog: Boolean = {
    val isAdmin = flags.exists(_.contains("admin"))
    val hasRole = roleName.nonEmpty

    // this will most likely need to change to include rights
    hasRole || isAdmin
  }
}

object User {
  implicit val jCodec = AutomaticJsonCodecBuilder[User]
}
