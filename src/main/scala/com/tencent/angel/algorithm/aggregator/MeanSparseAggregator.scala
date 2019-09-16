package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric

import scala.reflect.ClassTag

class MeanSparseAggregator[T: ClassTag](dim: Int,
                                        activation: String = "relu",
                                        concat: Boolean = false)
                                       (implicit ev: TensorNumeric[T]) extends BaseSparseAggregator[T] {
  override def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T], adj: ModuleNode[T]): ModuleNode[T] = {
    null
  }
}
