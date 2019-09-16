package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric

import scala.reflect.ClassTag

class MeanPoolAggregator[T: ClassTag](dim: Int,
                                      activation: String = "relu",
                                      concat: Boolean = false)
                                     (implicit ev: TensorNumeric[T]) extends BaseAggregator[T] {
  override def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T]): ModuleNode[T] = {
    null
  }
}
