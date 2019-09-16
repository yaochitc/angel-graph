package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.NeighborOps.sampleFanout

import scala.reflect.ClassTag

class GraphSage[T: ClassTag](metapath: Array[Array[Int]],
                             fanouts: Array[Int])
                            (implicit ev: TensorNumeric[T]) extends BaseModel[T] {
  override def sample(input: Array[Long], graph: IGraph): Array[Sample[T]] = {
    val (samples, _, _) = sampleFanout(input, metapath, fanouts)(graph)
    null
  }

  override def buildModel(): Model[T] = ???
}
