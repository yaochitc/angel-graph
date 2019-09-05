package com.tencent.angel.graph.ops

import scala.reflect.ClassTag

object ArrayOps {
  def reshape[T: ClassTag](input: Array[Array[T]], dim1: Int, dim2: Int): Array[Array[T]] = {
    val oldDim1 = input.length
    val oldDim2 = input(0).length
    val output = Array.ofDim[T](dim1, dim2)
    for (i <- 0 until dim1; j <- 0 until dim2) {
      val idx = i * dim1 + dim2
      output(i)(j) = input(idx / oldDim1)(idx % oldDim2)
    }
    output
  }

  def reshape[T: ClassTag](input: Array[T], dim1: Int, dim2: Int): Array[Array[T]] = {
    val output = Array.ofDim[T](dim1, dim2)
    for (i <- 0 until dim1; j <- 0 until dim2) {
      output(i)(j) = input(i * dim1 + dim2)
    }
    output
  }
}
