package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.Mean
import com.intel.analytics.bigdl.nn.keras.KerasLayerWrapper
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers.{Dense, Merge}

import scala.reflect.ClassTag

class MeanAggregator[T: ClassTag](dim: Int,
                                  activation: String = "relu",
                                  concat: Boolean = false)
                                 (implicit ev: TensorNumeric[T]) extends Aggregator[T] {
  private val meanLayer = new KerasLayerWrapper[T](Mean[T]())
  private val inputDenseLayer = Dense(dim, activation = activation, bias = false)
  private val neighborDenseLayer = Dense(dim, activation = activation, bias = false)
  private val mergeLayer = if (concat) Merge[T](mode = "concat") else Merge[T](mode = "sum")

  override def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T]): ModuleNode[T] = {
    val aggregated = meanLayer.inputs(neighbor)
    val inputEmbedding = inputDenseLayer.inputs(input)
    val neighborEmbedding = neighborDenseLayer.inputs(aggregated)

    mergeLayer.inputs(inputEmbedding, neighborEmbedding)
  }
}
