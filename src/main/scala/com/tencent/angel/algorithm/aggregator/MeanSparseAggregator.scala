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
                                       (implicit ev: TensorNumeric[T]) extends BaseSparseAggregator[T] {
  override def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T], adj: ModuleNode[T]): ModuleNode[T] = {
    val degree = new KerasLayerWrapper[T](Sum[T, T]()).inputs(adj)
    val norm = HardTanh[T](minValue = 1e-7,
      maxValue = Double.MaxValue).inputs(degree)

    val aggregated = new KerasLayerWrapper[T](MM[T]()).inputs(neighbor, degree)
    val normalized = new KerasLayerWrapper[T](CDivTable[T]()).inputs(aggregated, norm)

    val inputEmbedding = Dense(dim, activation = activation, bias = false).inputs(input)
    val neighborEmbedding = Dense(dim, activation = activation, bias = false).inputs(aggregated)

    val mode = if (concat) "concat" else "sum"

    Merge[T](mode = mode).inputs(inputEmbedding, neighborEmbedding)
  }
}
