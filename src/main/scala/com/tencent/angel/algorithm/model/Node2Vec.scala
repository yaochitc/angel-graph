package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils.Shape
import com.intel.analytics.zoo.pipeline.api.keras.layers.Input
import com.tencent.angel.algorithm.encoder.ShallowEncoder

import scala.reflect.ClassTag

class Node2Vec[T: ClassTag](maxId: Int, dim: Int, numNegs: Int = 5)
                           (implicit ev: TensorNumeric[T]) {
  def buildModel(): Unit = {
    val src = Input[T](inputShape = Shape(1))
    val pos = Input[T](inputShape = Shape(1))
    val neg = Input[T](inputShape = Shape(numNegs))

    val targetEncoder = ShallowEncoder[T](maxId, dim)
    val contextEncoder = ShallowEncoder[T](maxId, dim)

    val srcEmbedding = targetEncoder.encode(src)
    val posEmbedding = contextEncoder.encode(pos)
    val negEmbedding = contextEncoder.encode(neg)
  }
}
