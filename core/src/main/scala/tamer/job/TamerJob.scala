package tamer.job

import tamer.TamerError
import zio.ZIO

trait TamerJob[R] {
  def fetch(): ZIO[R, TamerError, Unit]
}
