package com.tencent.angel.algorithm.encoder

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.tencent.angel.algorithm.aggregator.Aggregator
import com.tencent.angel.algorithm.aggregator.AggregatorType.AggregatorType

import scala.reflect.ClassTag

class SageEncoder[T: ClassTag](numLayer: Int,
                               dim: Int,
                               aggregatorType: AggregatorType,
                               concat: Boolean,
                               maxId: Int,
                               embeddingDim: Int)
                              (implicit ev: TensorNumeric[T]) extends BaseEncoder[T, Seq[ModuleNode[T]]] {
  private val nodeEncoder = ShallowEncoder[T](dim, maxId, embeddingDim)

  private val aggregators = (0 until numLayer).map(i => {
    val activation = if (i < numLayer - 1) "relu" else null
    Aggregator(aggregatorType, dim, activation, concat)
  })

  override def encode(inputs: Seq[ModuleNode[T]], namePrefix: String, isReplica: Boolean): ModuleNode[T] = {
    var hidden = inputs.map(node => nodeEncoder.encode(node, namePrefix, isReplica))
    for (layer <- 0 until numLayer) {
      val aggregator = aggregators(layer)
      hidden = (0 until numLayer - layer)
        .map(hop => aggregator.aggregate(hidden(hop), hidden(hop + 1)))
    }
    hidden.head
  }
}
