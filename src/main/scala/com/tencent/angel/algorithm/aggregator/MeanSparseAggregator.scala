package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.{CDivTable, MM}
import com.intel.analytics.bigdl.nn.keras.KerasLayerWrapper
import com.intel.analytics.bigdl.nn.ops.Sum
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers.{Dense, HardTanh, Merge}

import scala.reflect.ClassTag

class MeanSparseAggregator[T: ClassTag](dim: Int,
                                        activation: String = "relu",
                                        concat: Boolean = false)
                                       (implicit ev: TensorNumeric[T]) extends SparseAggregator[T] {
  private val sumLayer = new KerasLayerWrapper[T](Sum[T, T]())
  private val maximumLayer = HardTanh[T](minValue = 1e-7,
    maxValue = Double.MaxValue)
  private val mmLayer = new KerasLayerWrapper[T](MM[T]())
  private val divLayer = new KerasLayerWrapper[T](CDivTable[T]())
  private val inputDenseLayer = Dense(dim, activation = activation, bias = false)
  private val neighborDenseLayer = Dense(dim, activation = activation, bias = false)
  private val mergeLayer = if (concat) Merge[T](mode = "concat") else Merge[T](mode = "sum")

  override def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T], adj: ModuleNode[T]): ModuleNode[T] = {
    val degree = sumLayer.inputs(adj)
    val norm = maximumLayer.inputs(degree)

    val aggregated = mmLayer.inputs(neighbor, degree)
    val normalized = divLayer.inputs(aggregated, norm)

    val inputEmbedding = inputDenseLayer.inputs(input)
    val neighborEmbedding = neighborDenseLayer.inputs(aggregated)

    mergeLayer.inputs(inputEmbedding, neighborEmbedding)
  }
}
