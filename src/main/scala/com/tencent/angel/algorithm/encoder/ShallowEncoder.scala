package com.tencent.angel.algorithm.encoder

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers._

import scala.reflect.ClassTag

class ShallowEncoder[T: ClassTag](dim: Int,
                                  maxId: Int,
                                  embeddingDim: Int)
                                 (implicit ev: TensorNumeric[T]) extends BaseEncoder[T, ModuleNode[T]] {
  private val embeddingLayer = Embedding[T](maxId, embeddingDim)
  private val denseLayer = Dense(dim, bias = false)

  override def encode(input: ModuleNode[T]): ModuleNode[T] = {
    val embedding = embeddingLayer.inputs(input)
    denseLayer.inputs(embedding)
  }
}

object ShallowEncoder {
  def apply[T: ClassTag](dim: Int, maxId: Int, embeddingDim: Int)(implicit ev: TensorNumeric[T]): ShallowEncoder[T] = {
    new ShallowEncoder[T](dim, maxId, embeddingDim)
  }
}