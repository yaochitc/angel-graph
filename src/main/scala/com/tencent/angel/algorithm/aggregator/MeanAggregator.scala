package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers.Dense

import scala.reflect.ClassTag

class MeanAggregator[T: ClassTag](dim: Int,
                                  activation: String = "relu",
                                  concat: Boolean = false)
                                 (implicit ev: TensorNumeric[T]) extends BaseAggregator[T] {
  val inputLayer = Dense(dim, activation = activation, bias = false)
  val neighborLayer = Dense(dim, activation = activation, bias = false)

  override def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T]): ModuleNode[T] = {
    null
  }
}
