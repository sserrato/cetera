package com.socrata.cetera

import scala.io.Source

import com.socrata.cetera.types.{EsUser, Role}

trait TestESUsers {

  val users = {
    val userTSV = Source.fromInputStream(getClass.getResourceAsStream("/users.tsv"))
    userTSV.getLines()
    val iter = userTSV.getLines().map(_.split("\t"))
    iter.drop(1) // drop the header columns
    val originalUsers = iter.map { tsvLine =>
      EsUser(
        id = tsvLine(0),
        screenName = Option(tsvLine(1)).filter(_.nonEmpty),
        email = Option(tsvLine(2)).filter(_.nonEmpty),
        roles = Some(Set(Role(tsvLine(3).toInt, tsvLine(4)))),
        flags = Option(List(tsvLine(5)).filter(_.nonEmpty)),
        profileImageUrlLarge = Option(tsvLine(6)).filter(_.nonEmpty),
        profileImageUrlMedium = Option(tsvLine(7)).filter(_.nonEmpty),
        profileImageUrlSmall = Option(tsvLine(8)).filter(_.nonEmpty)
      )
    }.toSeq

    // we want some users to have roles on mulitple domains
    originalUsers.map(u =>
      if (u.id == "bright-heart") {
        val moreRoles = u.roles.get ++ Some(Role(1, "honorary-bear"))
        u.copy(roles = Some(moreRoles))
      } else {
        u
      }
    )
  }
}
