package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.keras.KerasLayerWrapper
import com.intel.analytics.bigdl.nn.ops.Sum
import com.intel.analytics.bigdl.nn.{CAddTable, CDivTable, MM}
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers.{AddConstant, HardTanh}

import scala.reflect.ClassTag

class GCNSparseAggregator[T: ClassTag](dim: Int,
                                       activation: String = "relu",
                                       renorm: Boolean = false)
                                      (implicit ev: TensorNumeric[T]) extends BaseSparseAggregator[T] {
  override def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T], adj: ModuleNode[T]): ModuleNode[T] = {
    val degree = new KerasLayerWrapper[T](Sum[T, T]()).inputs(adj)
    val aggregated = new KerasLayerWrapper[T](MM[T]()).inputs(neighbor, degree)

    if (renorm) {
      val merged = new KerasLayerWrapper[T](CAddTable[T]()).inputs(input, aggregated)
      val norm = AddConstant(1).inputs(degree)
      new KerasLayerWrapper[T](CDivTable[T]()).inputs(aggregated, norm)
    } else {
      val norm = HardTanh[T](minValue = 1e-7,
        maxValue = Double.MaxValue).inputs(degree)
      val normalized = new KerasLayerWrapper[T](CDivTable[T]()).inputs(aggregated, norm)
      new KerasLayerWrapper[T](CAddTable[T]()).inputs(input, normalized)
    }
  }
}

