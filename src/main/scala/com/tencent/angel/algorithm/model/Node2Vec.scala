package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.nn.keras.KerasLayerWrapper
import com.intel.analytics.bigdl.nn.{MM, Sigmoid}
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils.Shape
import com.intel.analytics.zoo.pipeline.api.keras.layers.{Input, Merge}
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.algorithm.encoder.ShallowEncoder
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.WalkOps._
import com.tencent.angel.graph.ops.SampleOps._

import scala.reflect.ClassTag

class Node2Vec[T: ClassTag](nodeType: Int,
                            edgeTypes: Array[Int],
                            maxId: Int,
                            walkLen: Int,
                            p: Float,
                            q: Float,
                            leftWinSize: Int,
                            rightWinSize: Int,
                            dim: Int,
                            numNegs: Int = 5)
                           (implicit ev: TensorNumeric[T]) {
  private val pathLen = walkLen + 1
  private val numPairs = (pathLen - 1 until 0 by -1).map(i => Math.min(i, rightWinSize) + Math.min(i, leftWinSize)).sum

  def sample(input: Array[Long], graph: IGraph): Array[Sample[T]] = {
    val paths = randomWalk(input, edgeTypes, walkLen, p, q)(graph)
    val (src, pos) = genPair(paths, pathLen, numPairs, leftWinSize, rightWinSize)
    val batchSize = input.length
    val neg = sampleNode(nodeType, batchSize * numPairs * numNegs)(graph)

    val samples = Array.ofDim[Sample[T]](batchSize * numPairs)
    for (b <- 0 until batchSize; p <- 0 until numPairs) {
      val srcTenor = Tensor[T](1).setValue(1, ev.fromType(src(b)(p)))
      val posTenor = Tensor[T](1).setValue(1, ev.fromType(pos(b)(p)))
      val negTensor = Tensor[T](numNegs)
      for (n <- 0 until numNegs) {
        negTensor.setValue(n + 1, ev.fromType(neg(b * numPairs + p * numNegs + n)))
      }

      val labelTensor = Tensor[T](numNegs + 1).zero().setValue(1, ev.one)

      samples(b * numPairs + p) = Sample[T](Array(srcTenor, posTenor, negTensor), labelTensor)
    }

    samples
  }

  def buildModel(): Model[T] = {
    val src = Input[T](inputShape = Shape(1))
    val pos = Input[T](inputShape = Shape(1))
    val neg = Input[T](inputShape = Shape(numNegs))

    val targetEncoder = ShallowEncoder[T](maxId, dim)
    val contextEncoder = ShallowEncoder[T](maxId, dim)

    val srcEmbedding = targetEncoder.encode(src)
    val posEmbedding = contextEncoder.encode(pos)
    val negEmbedding = contextEncoder.encode(neg)

    val posLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, posEmbedding)
    val negLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, negEmbedding)

    val logit = Merge[T](mode = "concat").inputs(posLogit, negLogit)
    val output = new KerasLayerWrapper[T](Sigmoid[T]()).inputs(logit)
    Model[T](Array(src, pos, neg), output)
  }
}
