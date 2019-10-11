package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.{MiniBatch, SparseMiniBatch}
import com.intel.analytics.bigdl.nn.keras.KerasLayerWrapper
import com.intel.analytics.bigdl.nn.{MM, Sigmoid}
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils.Shape
import com.intel.analytics.zoo.pipeline.api.keras.layers.{Input, Merge}
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.algorithm.encoder.ShallowEncoder
import com.tencent.angel.algorithm.model.Order.Order
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.NeighborOps.sampleNeighbor
import com.tencent.angel.graph.ops.SampleOps.sampleNode

import scala.reflect.ClassTag

object Order extends Enumeration {
  type Order = Value
  val First: Value = Value(1)
  val Second: Value = Value(2)
}

class Line[T: ClassTag](nodeType: Int,
                        edgeTypes: Array[Int],
                        maxId: Int,
                        dim: Int,
                        embeddingDim: Int,
                        denseFeatureDim: Int,
                        sparseFeatureMaxIds: Array[Int],
                        order: Order,
                        numNegs: Int = 5)
                       (implicit ev: TensorNumeric[T]) extends BaseModel[T] {

  override def sample(input: Array[Long], graph: IGraph): MiniBatch[T] = {
    val batchSize = input.length
    val (pos, _, _) = sampleNeighbor(input, edgeTypes, 1, maxId + 1)(graph)
    val neg = sampleNode(nodeType, batchSize * numNegs)(graph)

    val srcTenor = Tensor[T](batchSize, 1)
    val posTenor = Tensor[T](batchSize, 1)
    val negTensor = Tensor[T](batchSize, numNegs)
    val labelTensor = Tensor[T](batchSize, numNegs + 1).zero()
    for (b <- 0 until batchSize) {
      srcTenor.setValue(b, 1, ev.fromType(input(b)))
      posTenor.setValue(b, 1, ev.fromType(pos(b)(0)))
      for (n <- 0 until numNegs) {
        negTensor.setValue(b, n + 1, ev.fromType(neg(b * numNegs + n)))
      }

      labelTensor.setValue(batchSize, 1, ev.one)
    }

    new SparseMiniBatch[T](Array(srcTenor, posTenor, negTensor), Array(labelTensor))
  }

  override def buildModel(): Model[T] = {
    val src = Input[T](Shape(1))
    val srcFeature = Input[T](Shape(denseFeatureDim))
    val srcSparseFeature = sparseFeatureMaxIds.map(sparseFeatureMaxId => Input[T](Shape(sparseFeatureMaxId)))
    val pos = Input[T](Shape(1))
    val posFeature = Input[T](Shape(denseFeatureDim))
    val posSparseFeature = sparseFeatureMaxIds.map(sparseFeatureMaxId => Input[T](Shape(sparseFeatureMaxId)))
    val neg = Input[T](Shape(numNegs))
    val negFeature = Input[T](Shape(denseFeatureDim))
    val negSparseFeature = sparseFeatureMaxIds.map(sparseFeatureMaxId => Input[T](Shape(sparseFeatureMaxId)))

    val targetEncoder = ShallowEncoder[T](dim, maxId, embeddingDim, denseFeatureDim, sparseFeatureMaxIds)
    val srcEmbedding = targetEncoder.encode((src, srcFeature, srcSparseFeature), "src", isReplica = false)

    val (posEmbedding, negEmbedding) = order match {
      case Order.First =>
        (targetEncoder.encode((pos, posFeature, posSparseFeature), "pos", isReplica = true), targetEncoder.encode((neg, negFeature, negSparseFeature), "neg", isReplica = true))
      case Order.Second =>
        val contextEncoder = ShallowEncoder[T](dim, maxId, embeddingDim, denseFeatureDim, sparseFeatureMaxIds)
        (contextEncoder.encode((pos, posFeature, posSparseFeature), "pos", isReplica = false), contextEncoder.encode((neg, negFeature, negSparseFeature), "neg", isReplica = true))
    }

    val posLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, posEmbedding)
    val negLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, negEmbedding)

    val logit = Merge[T](mode = "concat").inputs(posLogit, negLogit)
    val output = new KerasLayerWrapper[T](Sigmoid[T]()).inputs(logit)
    Model[T](Array(src, pos, neg), output)
  }
}
