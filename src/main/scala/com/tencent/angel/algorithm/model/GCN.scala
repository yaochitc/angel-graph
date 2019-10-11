package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.MiniBatch
import com.intel.analytics.bigdl.nn.SoftMax
import com.intel.analytics.bigdl.nn.keras.{Input, KerasLayerWrapper}
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers.Dense
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
                       denseFeatureDim: Int,
                       sparseFeatureMaxIds: Array[Int],
                       numClasses: Int,
                       useResidual: Boolean = false)
                      (implicit ev: TensorNumeric[T]) extends BaseModel[T] {
  private val numLayer = metapath.length

  override def sample(input: Array[Long], graph: IGraph): MiniBatch[T] = {
    val batchSize = input.length
    val (nodes, adjs) = getMultiHopNeighbor(input, metapath)(graph)
    null
  }

  override def buildModel(): Model[T] = {
    val inputs = (0 until numLayer + 1).map(_ => Input[T]()).toArray
    val adjs = (0 until numLayer).map(_ => Input[T]()).toArray

    val encoder = new GCNEncoder[T](numLayer, dim, aggregatorType, maxId, embeddingDim, denseFeatureDim, sparseFeatureMaxIds, useResidual)
    val embedding = encoder.encode(inputs, adjs)

    val logit = Dense[T](numClasses).inputs(embedding)
    val output = new KerasLayerWrapper[T](SoftMax[T]()).inputs(logit)
    Model(inputs ++ adjs, output)
  }
}
