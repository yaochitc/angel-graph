package com.tencent.angel.algorithm.encoder

import com.intel.analytics.bigdl.nn.CAddTable
import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.keras.KerasLayerWrapper
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.tencent.angel.algorithm.aggregator.SparseAggregator
import com.tencent.angel.algorithm.aggregator.SparseAggregatorType.SparseAggregatorType

import scala.reflect.ClassTag

class GCNEncoder[T: ClassTag](numLayer: Int,
                              dim: Int,
                              aggregatorType: SparseAggregatorType,
                              maxId: Int,
                              embeddingDim: Int,
                              useResidual: Boolean)
                             (implicit ev: TensorNumeric[T]) extends BaseEncoder[T, (Seq[ModuleNode[T]], Seq[ModuleNode[T]])] {
  private val nodeEncoder = ShallowEncoder[T](dim, maxId, embeddingDim)

  private val aggregators = (0 until numLayer).map(i => {
    val activation = if (i < numLayer - 1) "relu" else null
    SparseAggregator(aggregatorType, dim, activation)
  })

  override def encode(input: (Seq[ModuleNode[T]], Seq[ModuleNode[T]])): ModuleNode[T] = {
    val (nodes, adjs) = input
    var hidden = nodes.map(node => nodeEncoder.encode(node))
    for (layer <- 0 until numLayer) {
      val aggregator = aggregators(layer)
      hidden = (0 until numLayer - layer)
        .map(hop => {
          val aggregated = aggregator.aggregate(hidden(hop), hidden(hop + 1), adjs(hop))
          if (useResidual) {
            val addLayer = new KerasLayerWrapper[T](CAddTable[T]())
            addLayer.inputs(hidden(hop), aggregated)
          } else {
            aggregated
          }
        })
    }
    hidden.head
  }
}
