package com.tencent.angel.algorithm.encoder

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.zoo.pipeline.api.keras.layers._

import scala.reflect.ClassTag

class ShallowEncoder[T: ClassTag](maxId: Int,
                                  dim: Int)
                                 (implicit ev: TensorNumeric[T]) extends BaseEncoder[T] {
  private val embedding = Embedding[T](maxId, dim)

  override def encode(input: ModuleNode[T]): ModuleNode[T] = {
    embedding.inputs(input)
  }
}

object ShallowEncoder {
  def apply[T: ClassTag](maxId: Int, dim: Int)(implicit ev: TensorNumeric[T]): ShallowEncoder[T] = {
    new ShallowEncoder[T](maxId, dim)
  }
}