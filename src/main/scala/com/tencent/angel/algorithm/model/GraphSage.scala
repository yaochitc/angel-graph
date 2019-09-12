package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.NeighborOps.sampleFanout

import scala.reflect.ClassTag

class GraphSage[T: ClassTag](metapath: Array[Array[Int]],
                             fanouts: Array[Int])
                            (implicit ev: TensorNumeric[T]) {
  def sample(input: Array[Long], graph: IGraph): Array[Sample[T]] = {
    sampleFanout(input, metapath, fanouts)(graph)
    null
  }
}
