package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode

trait BaseAggregator[T] {
  def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T]): ModuleNode[T]
}
