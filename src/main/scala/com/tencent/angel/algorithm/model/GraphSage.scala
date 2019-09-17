package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.nn.keras.Input
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils.Shape
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.algorithm.encoder.ShallowEncoder
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.NeighborOps.sampleFanout

import scala.reflect.ClassTag

class GraphSage[T: ClassTag](metapath: Array[Array[Int]],
                             fanouts: Array[Int],
                             maxId: Int,
                             embeddingDim: Int)
                            (implicit ev: TensorNumeric[T]) extends BaseModel[T] {
  private val numLayer = fanouts.length

  override def sample(input: Array[Long], graph: IGraph): Array[Sample[T]] = {
    val (samples, _, _) = sampleFanout(input, metapath, fanouts)(graph)
    null
  }

  override def buildModel(): Model[T] = {
    val inputs = fanouts.map(fanout => Input[T](inputShape = Shape(fanout)))
    val nodeEncoder = ShallowEncoder[T](maxId, embeddingDim)

    var hidden = inputs.map(input => nodeEncoder.encode(input))


    null
  }
}
