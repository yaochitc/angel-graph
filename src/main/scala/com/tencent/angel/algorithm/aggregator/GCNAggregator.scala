package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.Mean
import com.intel.analytics.bigdl.nn.keras.KerasLayerWrapper
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers.{Dense, ExpandDim, Merge}

import scala.reflect.ClassTag

class GCNAggregator[T: ClassTag](dim: Int,
                                 activation: String = "relu")
                                (implicit ev: TensorNumeric[T]) extends Aggregator[T] {

  private val mergeLayer = Merge[T](mode = "concat")
  private val meanLayer = new KerasLayerWrapper[T](Mean[T]())
  private val denseLayer = Dense(dim, activation = activation, bias = false)

  override def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T]): ModuleNode[T] = {
    val merged = mergeLayer.inputs(ExpandDim(1).inputs(input), neighbor)
    val aggregated = meanLayer.inputs(merged)

    denseLayer.inputs(aggregated)
  }
}
