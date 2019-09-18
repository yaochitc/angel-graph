package com.tencent.angel.algorithm.encoder

import com.intel.analytics.bigdl.nn.Graph.ModuleNode

trait BaseEncoder[T, I] {
  def encode(input: I): ModuleNode[T]
}
