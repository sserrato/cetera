package com.socrata.cetera.types

case class ApprovalStatus(status: String, booleanValue: Option[Boolean])

object ApprovalStatus {
  val approved = ApprovalStatus("approved", Some(true))
  val pending = ApprovalStatus("pending", None)
  val rejected = ApprovalStatus("rejected", Some(false))

  val all = List(approved, pending, rejected)
}
