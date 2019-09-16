package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode

trait Aggregator[T] {
  def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T]): ModuleNode[T]
}
