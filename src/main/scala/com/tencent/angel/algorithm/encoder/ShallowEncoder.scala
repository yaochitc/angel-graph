package com.tencent.angel.algorithm.encoder

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers._
import com.tencent.angel.algorithm.nn.keras.ReusableLayer

import scala.reflect.ClassTag

class ShallowEncoder[T: ClassTag](dim: Int,
                                  maxId: Int,
                                  embeddingDim: Int)
                                 (implicit ev: TensorNumeric[T]) extends BaseEncoder[T, ModuleNode[T]] {
  private val embeddingLayer = ReusableLayer[T](Embedding[T](maxId, embeddingDim), hasGradInput = false)
  private val denseLayer = ReusableLayer[T](Dense(dim, bias = false))

  override def encode(input: ModuleNode[T], namePrefix: String, isReplica: Boolean): ModuleNode[T] = {
    val embedding = embeddingLayer.copy(namePrefix + "_embedding", isReplica).inputs(input)
    denseLayer.copy(namePrefix + "_dense", isReplica).inputs(embedding)
  }
}

object ShallowEncoder {
  def apply[T: ClassTag](dim: Int, maxId: Int, embeddingDim: Int)(implicit ev: TensorNumeric[T]): ShallowEncoder[T] = {
    new ShallowEncoder[T](dim, maxId, embeddingDim)
  }
}