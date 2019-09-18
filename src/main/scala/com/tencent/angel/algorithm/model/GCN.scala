package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.MiniBatch
import com.intel.analytics.bigdl.nn.keras.{Input, KerasLayerWrapper}
import com.intel.analytics.bigdl.nn.{MM, Sigmoid}
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers.Merge
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.algorithm.aggregator.SparseAggregatorType.SparseAggregatorType
import com.tencent.angel.algorithm.encoder.GCNEncoder
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

    val contextEncoder = new GCNEncoder[T](numLayer, dim, aggregatorType, maxId, embeddingDim, useResidual)
    val targetEncoder = new GCNEncoder[T](numLayer, dim, aggregatorType, maxId, embeddingDim, useResidual)

    val srcEmbedding = targetEncoder.encode(inputs, adjs)
    val posEmbedding = contextEncoder.encode(inputs, adjs)
    val negEmbedding = contextEncoder.encode(inputs, adjs)

    val posLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, posEmbedding)
    val negLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, negEmbedding)

    val logit = Merge[T](mode = "concat").inputs(posLogit, negLogit)
    val output = new KerasLayerWrapper[T](Sigmoid[T]()).inputs(logit)
    Model((inputs ++ adjs).toArray, output)
  }
}
