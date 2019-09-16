package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.keras.Input
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.algorithm.aggregator.SparseAggregator
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.NeighborOps.getMultiHopNeighbor

import scala.reflect.ClassTag

class GCN[T: ClassTag](metapath: Array[Array[Int]])
                      (implicit ev: TensorNumeric[T]) extends BaseModel[T] {
  private val numLayer = metapath.length

  override def sample(input: Array[Long], graph: IGraph): Array[Sample[T]] = {
    val (nodes, adjs) = getMultiHopNeighbor(input, metapath)(graph)
    null
  }

  override def buildModel(): Model[T] = {
    val inputs = Array.ofDim[ModuleNode[T]](numLayer)
    val aggregators = Array.ofDim[SparseAggregator[T]](numLayer)

    for (i <- 0 until numLayer) {
      inputs(i) = Input[T]()
    }


    null
  }
}
