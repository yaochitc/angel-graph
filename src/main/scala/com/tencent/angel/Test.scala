package com.tencent.angel

import com.intel.analytics.bigdl.dataset.{SparseMiniBatch, TensorSample}
import com.intel.analytics.bigdl.tensor.Tensor

object Test {
  def main(args: Array[String]): Unit = {
//    val a1 = Tensor.sparse(Tensor[Float](5).range(1, 5, 1))
//    val a2 = Tensor.sparse(Tensor[Float](4).range(5, 8, 1))
//    val b1 = Tensor[Float](5).range(1, 5, 1)
//    val b2 = Tensor[Float](5).range(6, 10, 1)
//    val c1 = Tensor[Float](1).fill(1)
//    val c2 = Tensor[Float](1).fill(0)
//    val sample1 = TensorSample[Float](Array(b1), Array(c1))
//    val sample2 = TensorSample[Float](Array(b2), Array(c2))
//    val miniBatch = SparseMiniBatch[Float](1, 1)
//    miniBatch.set(Array(sample1, sample2))
//
//    val input = miniBatch.getInput()
//    val target = miniBatch.getTarget()
//    println()
    val a = Array(2,3,4,5)
    val b  = a.scanLeft(1){case (acc, item) => acc*item}
    println(b)
  }
}
