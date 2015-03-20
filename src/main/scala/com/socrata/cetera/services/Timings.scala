package com.socrata.cetera.services

import java.util.concurrent.TimeUnit

object Timings {
  def now(): Long = System.nanoTime()
  def elapsedInMillis(previous:Long): Long = TimeUnit.NANOSECONDS.toMillis(now()-previous)
}
