package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.MiniBatch
import com.intel.analytics.bigdl.nn.keras.{Input, KerasLayerWrapper}
import com.intel.analytics.bigdl.nn.{MM, Sigmoid}
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils.Shape
import com.intel.analytics.zoo.pipeline.api.keras.layers.Merge
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.algorithm.aggregator.AggregatorType.AggregatorType
import com.tencent.angel.algorithm.encoder.SageEncoder
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
  private val countPerLayer = fanouts.scanLeft(1) { case (acc, item) => acc * item }
  private val numLayer = fanouts.length

  override def sample(input: Array[Long], graph: IGraph): MiniBatch[T] = {
    val batchSize = input.length
    val (nodes, _, _) = sampleFanout(input, metapath, fanouts)(graph)
    val featureTensors = (0 until numLayer + 1).map(i => Tensor[T](batchSize, countPerLayer(i)))

    for (i <- 0 until numLayer + 1; b <- 0 until batchSize; count <- 0 until countPerLayer(i)) {
      featureTensors(i).setValue(b, count, ev.fromType(nodes(i)(b * countPerLayer(i) + count)))
    }
    null
  }

  override def buildModel(): Model[T] = {
    val inputs = countPerLayer.map(count => Input[T](inputShape = Shape(count)))

    val contextEncoder = new SageEncoder[T](numLayer, dim, aggregatorType, concat, maxId, embeddingDim)
    val targetEncoder = new SageEncoder[T](numLayer, dim, aggregatorType, concat, maxId, embeddingDim)

    val srcEmbedding = targetEncoder.encode(inputs)
    val posEmbedding = contextEncoder.encode(inputs)
    val negEmbedding = contextEncoder.encode(inputs)

    val posLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, posEmbedding)
    val negLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, negEmbedding)

    val logit = Merge[T](mode = "concat").inputs(posLogit, negLogit)
    val output = new KerasLayerWrapper[T](Sigmoid[T]()).inputs(logit)
    Model(inputs, output)
  }
}
