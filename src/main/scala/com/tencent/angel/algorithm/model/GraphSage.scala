package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.nn.keras.Input
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils.Shape
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.algorithm.aggregator.Aggregator
import com.tencent.angel.algorithm.aggregator.AggregatorType.AggregatorType
import com.tencent.angel.algorithm.encoder.ShallowEncoder
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.NeighborOps.sampleFanout

import scala.reflect.ClassTag

class GraphSage[T: ClassTag](metapath: Array[Array[Int]],
                             fanouts: Array[Int],
                             dim: Int,
                             aggregatorType: AggregatorType,
                             concat: Boolean,
                             maxId: Int,
                             embeddingDim: Int)
                            (implicit ev: TensorNumeric[T]) extends BaseModel[T] {
  private val numLayer = fanouts.length

  override def sample(input: Array[Long], graph: IGraph): Array[Sample[T]] = {
    val (samples, _, _) = sampleFanout(input, metapath, fanouts)(graph)
    null
  }

  override def buildModel(): Model[T] = {
    val inputs = (0 until numLayer + 1).map(i =>
      if (i == 0) Input[T](inputShape = Shape(1)) else Input[T](inputShape = Shape(fanouts(i - 1)))
    )
    val aggregators = (0 until numLayer).map(i => {
      val activation = if (i < numLayer - 1) "relu" else null
      Aggregator(aggregatorType, dim, activation, concat)
    })

    val nodeEncoder = ShallowEncoder[T](dim, maxId, embeddingDim)

    var hidden = inputs.map(input => nodeEncoder.encode(input))
    for (layer <- 0 until numLayer) {
      val aggregator = aggregators(layer)
      hidden = (0 until numLayer - layer)
        .map(hop => aggregator.aggregate(hidden(hop), hidden(hop + 1)))
    }

    Model(inputs.toArray, hidden(0))
  }
}
