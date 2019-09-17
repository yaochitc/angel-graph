package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.{MiniBatch, Sample, SparseMiniBatch}
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
                        order: Order,
                        numNegs: Int = 5)
                       (implicit ev: TensorNumeric[T]) extends BaseModel[T] {

  override def sample(input: Array[Long], graph: IGraph): MiniBatch[T] = {
    val batchSize = input.length
    val (pos, _, _) = sampleNeighbor(input, edgeTypes, 1, maxId + 1)(graph)
    val neg = sampleNode(nodeType, batchSize * numNegs)(graph)

    val samples = Array.ofDim[Sample[T]](batchSize)
    for (b <- 0 until batchSize) {
      val srcTenor = Tensor[T](1).setValue(1, ev.fromType(input(b)))
      val posTenor = Tensor[T](1).setValue(1, ev.fromType(pos(b)(0)))
      val negTensor = Tensor[T](numNegs)
      for (n <- 0 until numNegs) {
        negTensor.setValue(n + 1, ev.fromType(neg(b * numNegs + n)))
      }

      val labelTensor = Tensor[T](numNegs + 1).zero().setValue(1, ev.one)

      samples(b) = Sample[T](Array(srcTenor, posTenor, negTensor), labelTensor)
    }

    val miniBatch = SparseMiniBatch[T](3, 1)
    miniBatch.set(samples)
  }

  override def buildModel(): Model[T] = {
    val src = Input[T](inputShape = Shape(1))
    val pos = Input[T](inputShape = Shape(1))
    val neg = Input[T](inputShape = Shape(numNegs))

    val targetEncoder = ShallowEncoder[T](dim, maxId, embeddingDim)

    val contextEncoder = order match {
      case Order.First => targetEncoder
      case Order.Second => ShallowEncoder[T](dim, maxId, embeddingDim)
    }

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
