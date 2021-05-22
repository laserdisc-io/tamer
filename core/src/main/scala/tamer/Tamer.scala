package tamer

import zio.ZIO

trait Tamer[-R] {
  def run: ZIO[R, TamerError, Unit]
}
