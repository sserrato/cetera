package com.socrata.cetera.auth

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

import com.socrata.cetera.types.Domain

case class User(
    id: String,
    authenticatingDomain: Option[Domain] = None,
    roleName: Option[String] = None,
    rights: Option[Seq[String]] = None,
    flags: Option[Seq[String]] = None) {

  def hasRole: Boolean = roleName.exists(_.nonEmpty)
  def hasRole(role: String): Boolean = roleName.exists(r => r.startsWith(role))
  def hasOneOfRoles(roles: Seq[String]): Boolean = roles.map(hasRole(_)).fold(false)(_ || _)
  def isSuperAdmin: Boolean = flags.exists(_.contains("admin"))
  def isAdmin: Boolean = hasRole("administrator") || isSuperAdmin

  def authorizedOnDomain(domainId: Int): Boolean = {
    authenticatingDomain.exists(_.domainId == domainId) || isSuperAdmin
  }

  def canViewResource(domainId: Int, isAuthorized: Boolean): Boolean = {
    authenticatingDomain match {
      case None => isSuperAdmin
      case Some(d) => (isAuthorized && authorizedOnDomain(domainId)) || isSuperAdmin
    }
  }

  def canViewLockedDownCatalog(domainId: Int): Boolean =
    canViewResource(domainId, hasOneOfRoles(Seq("editor", "publisher", "viewer", "administrator")))

  def canViewPrivateMetadata(domainId: Int): Boolean =
    canViewResource(domainId, hasOneOfRoles(Seq("publisher", "administrator")))

  def canViewAllViews(domainId: Int): Boolean =
    canViewResource(domainId, hasOneOfRoles(Seq("publisher", "designer", "viewer", "administrator")))

  def canViewAllUsers: Boolean =
    authenticatingDomain.exists(d => canViewResource(d.domainId, isAdmin)) || isSuperAdmin

  def canViewDomainUsers: Boolean =
    authenticatingDomain.exists(d => canViewResource(d.domainId, hasRole)) || isSuperAdmin

  def canViewUsers(domainId: Int): Boolean =
    canViewResource(domainId, hasRole)
}

object User {
  implicit val jCodec = AutomaticJsonCodecBuilder[User]

  def apply(j: JValue): Option[User] = JsonDecode.fromJValue[User](j).fold(_ => None, Some(_))
}
