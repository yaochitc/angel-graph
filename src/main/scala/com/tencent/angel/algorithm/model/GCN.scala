package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.NeighborOps.getMultiHopNeighbor

import scala.reflect.ClassTag

class GCN[T: ClassTag](metapath: Array[Array[Int]])
                      (implicit ev: TensorNumeric[T]) {
  def sample(input: Array[Long], graph: IGraph): Array[Sample[T]] = {
    val (nodes, adjs) = getMultiHopNeighbor(input, metapath)(graph)

    null
  }
}
