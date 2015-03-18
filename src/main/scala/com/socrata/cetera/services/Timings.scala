package com.socrata.cetera.services

import java.util.concurrent.TimeUnit

object Timings {
  def now() = System.nanoTime()
  def elapsedInMillis(previous:Long) = TimeUnit.NANOSECONDS.toMillis(now()-previous)
}
