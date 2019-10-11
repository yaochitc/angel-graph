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
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.SampleOps._
import com.tencent.angel.graph.ops.WalkOps._

import scala.reflect.ClassTag

class Node2Vec[T: ClassTag](nodeType: Int,
                            edgeTypes: Array[Int],
                            maxId: Int,
                            dim: Int,
                            walkLen: Int,
                            p: Float,
                            q: Float,
                            leftWinSize: Int,
                            rightWinSize: Int,
                            embeddingDim: Int,
                            denseFeatureDim: Int,
                            sparseFeatureMaxIds: Array[Int],
                            numNegs: Int = 5)
                           (implicit ev: TensorNumeric[T]) extends BaseModel[T] {
  private val pathLen = walkLen + 1
  private val numPairs = (pathLen - 1 until 0 by -1).map(i => Math.min(i, rightWinSize) + Math.min(i, leftWinSize)).sum

  override def sample(input: Array[Long], graph: IGraph): MiniBatch[T] = {
    val paths = randomWalk(input, edgeTypes, walkLen, p, q)(graph)
    val (src, pos) = genPair(paths, pathLen, numPairs, leftWinSize, rightWinSize)
    val batchSize = input.length
    val neg = sampleNode(nodeType, batchSize * numPairs * numNegs)(graph)

    val srcTenor = Tensor[T](batchSize * numPairs, 1)
    val posTenor = Tensor[T](batchSize * numPairs, 1)
    val negTensor = Tensor[T](batchSize * numPairs, numNegs)
    val labelTensor = Tensor[T](batchSize * numPairs, numNegs + 1).zero()

    for (b <- 0 until batchSize; p <- 0 until numPairs) {
      srcTenor.setValue(b * numPairs + p, 1, ev.fromType(src(b)(p)))
      posTenor.setValue(b * numPairs + p, 1, ev.fromType(pos(b)(p)))
      for (n <- 0 until numNegs) {
        negTensor.setValue(b * numPairs + p, n + 1, ev.fromType(neg(b * numPairs + p * numNegs + n)))
      }

      labelTensor.setValue(batchSize * numPairs, 1, ev.one)
    }

    new SparseMiniBatch[T](Array(srcTenor, posTenor, negTensor), Array(labelTensor))
  }

  override def buildModel(): Model[T] = {
    val src = Input[T](inputShape = Shape(1))
    val pos = Input[T](inputShape = Shape(1))
    val neg = Input[T](inputShape = Shape(numNegs))

    val targetEncoder = ShallowEncoder[T](dim, maxId, embeddingDim, denseFeatureDim, sparseFeatureMaxIds)
    val contextEncoder = ShallowEncoder[T](dim, maxId, embeddingDim, denseFeatureDim, sparseFeatureMaxIds)

    val srcEmbedding = targetEncoder.encode((src, null, null), "src", isReplica = false)
    val posEmbedding = contextEncoder.encode((pos, null, null), "pos", isReplica = true)
    val negEmbedding = contextEncoder.encode((neg, null, null), "neg", isReplica = true)

    val posLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, posEmbedding)
    val negLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, negEmbedding)

    val logit = Merge[T](mode = "concat").inputs(posLogit, negLogit)
    val output = new KerasLayerWrapper[T](Sigmoid[T]()).inputs(logit)
    Model[T](Array(src, pos, neg), output)
  }
}
