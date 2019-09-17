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
                                      (implicit ev: TensorNumeric[T]) extends SparseAggregator[T] {
  private val sumLayer = new KerasLayerWrapper[T](Sum[T, T]())
  private val mmLayer = new KerasLayerWrapper[T](MM[T]())

  private val addLayer = new KerasLayerWrapper[T](CAddTable[T]())
  private val maximumLayer = HardTanh[T](minValue = 1e-7,
    maxValue = Double.MaxValue)
  private val caddLayer = AddConstant(1)
  private val divLayer = new KerasLayerWrapper[T](CDivTable[T]())


  override def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T], adj: ModuleNode[T]): ModuleNode[T] = {
    val degree = sumLayer.inputs(adj)
    val aggregated = mmLayer.inputs(neighbor, adj)

    if (renorm) {
      val merged = addLayer.inputs(input, aggregated)
      val norm = caddLayer.inputs(degree)
      divLayer.inputs(aggregated, norm)
    } else {
      val norm = maximumLayer.inputs(degree)
      val normalized = divLayer.inputs(aggregated, norm)
      addLayer.inputs(input, normalized)
    }
  }
}

