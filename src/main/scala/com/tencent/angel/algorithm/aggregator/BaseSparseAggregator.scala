package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode

trait BaseSparseAggregator[T] {
  def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T], adj: ModuleNode[T]): ModuleNode[T]
}
