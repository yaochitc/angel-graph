package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.MiniBatch
import com.intel.analytics.bigdl.nn.CAddTable
import com.intel.analytics.bigdl.nn.keras.{Input, KerasLayerWrapper}
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.algorithm.aggregator.SparseAggregator
import com.tencent.angel.algorithm.aggregator.SparseAggregatorType.SparseAggregatorType
import com.tencent.angel.algorithm.encoder.ShallowEncoder
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.NeighborOps.getMultiHopNeighbor

import scala.reflect.ClassTag

class GCN[T: ClassTag](metapath: Array[Array[Int]],
                       aggregatorType: SparseAggregatorType,
                       dim: Int,
                       maxId: Int,
                       embeddingDim: Int,
                       useResidual: Boolean = false)
                      (implicit ev: TensorNumeric[T]) extends BaseModel[T] {
  private val numLayer = metapath.length

  override def sample(input: Array[Long], graph: IGraph): MiniBatch[T] = {
    val (nodes, adjs) = getMultiHopNeighbor(input, metapath)(graph)

    null
  }

  override def buildModel(): Model[T] = {
    val inputs = (0 until numLayer + 1).map(_ => Input[T]())
    val adjs = (0 until numLayer).map(_ => Input[T]())
    val aggregators = (0 until numLayer).map(i => {
      val activation = if (i < numLayer - 1) "relu" else null
      SparseAggregator(aggregatorType, dim, activation)
    })

    val nodeEncoder = ShallowEncoder[T](dim, maxId, embeddingDim)

    var hidden = inputs.map(input => nodeEncoder.encode(input))
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

    Model((inputs ++ adjs).toArray, hidden(0))
  }
}
