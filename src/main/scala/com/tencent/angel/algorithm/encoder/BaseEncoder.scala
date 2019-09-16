package com.tencent.angel.algorithm.encoder

import com.intel.analytics.bigdl.nn.Graph.ModuleNode

trait BaseEncoder[T] {
  def encode(input: ModuleNode[T]): ModuleNode[T]
}
