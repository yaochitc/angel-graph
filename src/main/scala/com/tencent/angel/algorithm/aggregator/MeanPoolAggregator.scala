package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.Mean
import com.intel.analytics.bigdl.nn.keras.KerasLayerWrapper
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers.{Dense, Merge}

import scala.reflect.ClassTag

class MeanPoolAggregator[T: ClassTag](dim: Int,
                                      activation: String = "relu",
                                      concat: Boolean = false)
                                     (implicit ev: TensorNumeric[T]) extends BaseAggregator[T] {
  override def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T]): ModuleNode[T] = {
    val pooled = Dense(dim, activation = activation).inputs(neighbor)
    val aggregated =  new KerasLayerWrapper[T](Mean[T]()).inputs(pooled)
    val inputEmbedding = Dense(dim, activation = activation, bias = false).inputs(input)
    val neighborEmbedding = Dense(dim, activation = activation, bias = false).inputs(aggregated)

    val mode = if (concat) "concat" else "sum"

    Merge[T](mode = mode).inputs(inputEmbedding, neighborEmbedding)
  }
}
