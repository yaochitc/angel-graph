package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.nn.keras.KerasLayerWrapper
import com.intel.analytics.bigdl.nn.{MM, Sigmoid}
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils.Shape
import com.intel.analytics.zoo.pipeline.api.keras.layers.{Input, Merge}
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.algorithm.encoder.ShallowEncoder
import com.tencent.angel.algorithm.model.Order.Order

import scala.reflect.ClassTag

object Order extends Enumeration {
  type Order = Value
  val First: Value = Value(1)
  val Second: Value = Value(2)
}

class Line[T: ClassTag](maxId: Int, dim: Int, order: Order, numNegs: Int = 5)
                       (implicit ev: TensorNumeric[T]) {
  def buildModel(): Unit = {
    val src = Input[T](inputShape = Shape(1))
    val pos = Input[T](inputShape = Shape(1))
    val neg = Input[T](inputShape = Shape(numNegs))

    val targetEncoder = ShallowEncoder[T](maxId, dim)

    val contextEncoder = order match {
      case Order.First => targetEncoder
      case Order.Second => ShallowEncoder[T](maxId, dim)
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
