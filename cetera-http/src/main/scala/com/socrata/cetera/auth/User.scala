package com.socrata.cetera.auth

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

case class User(
    id: String,
    roleName: Option[String],
    rights: Option[Seq[String]],
    flags: Option[Seq[String]]) {

  def hasRole: Boolean = roleName.nonEmpty
  def hasRole(role: String): Boolean = roleName.contains(role)
  def hasOneOfRoles(roles: Seq[String]): Boolean = roles.map(hasRole(_)).fold(false)(_ || _)
  def hasSuperAdminFlag: Boolean = flags.exists(_.contains("admin"))
  def isAdmin: Boolean = hasRole("administrator") || hasSuperAdminFlag

  def canViewLockedDownCatalog: Boolean = hasOneOfRoles(Seq("editor", "publisher", "viewer")) || isAdmin
  def canViewAdminDatasets: Boolean = hasRole("publisher") || isAdmin
  def canViewAssetSelector: Boolean = hasOneOfRoles(Seq("editor", "designer", "publisher", "viewer")) || isAdmin
  def canViewUsers: Boolean = isAdmin
}

object User {
  implicit val jCodec = AutomaticJsonCodecBuilder[User]

  def apply(j: JValue): Option[User] = JsonDecode.fromJValue[User](j).fold(_ => None, Some(_))
}
