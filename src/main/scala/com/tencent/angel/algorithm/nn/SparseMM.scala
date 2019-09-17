package com.tencent.angel.algorithm.nn

import com.intel.analytics.bigdl.nn.MM
import com.intel.analytics.bigdl.nn.abstractnn.AbstractModule
import com.intel.analytics.bigdl.tensor.{SparseTensorMath, Tensor}
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils.{T, Table}

import scala.reflect.ClassTag

class SparseMM[T: ClassTag]
(implicit ev: TensorNumeric[T]) extends AbstractModule[Table, Tensor[T], T] {
  gradInput = T(Tensor[T], Tensor[T]())

  private def checkInputFormat(input: Table): (Tensor[T], Tensor[T]) = {
    require(input.length() == 2 && input(1).isInstanceOf[Tensor[T]] &&
      input(2).isInstanceOf[Tensor[T]], "Input must be two tensors")
    val m1: Tensor[T] = input(1)
    val m2: Tensor[T] = input(2)
    require(m1.dim() == 2, "input matrix must be 2D" +
      s"input dim ${m1.dim()}")
    require(m2.dim() == 2, "input matrix must be 2D" +
      s"input dim ${m2.dim()}")

    (m1, m2)
  }

  override def updateOutput(input: Table): Tensor[T] = {
    val (ma, mb) = checkInputFormat(input)

    require(mb.dim() == 2, "second input tensor must be 2D" +
      s"second input dim ${mb.dim()}")

    require(ma.size(2) == mb.size(1), "matrix sizes do not match" +
      s"The sizes are ${ma.size(2)} and ${mb.size(1)}")

    SparseTensorMath.addmm(output, ev.zero, output, ev.one, mb, ma)
    output
  }

  override def updateGradInput(input: Table, gradOutput: Tensor[T]): Table = {

    val (ma, mb) = checkInputFormat(input)

    gradInput[Tensor[T]](1).resizeAs(ma)
    gradInput[Tensor[T]](2).resizeAs(mb)

    require(gradOutput.dim() == 2,
      "arguments must be a 2D Tensor" +
        s"arguments dim ${gradOutput.dim()}")


    val gradA = gradInput[Tensor[T]](1)
    val gradB = gradInput[Tensor[T]](2)

    SparseTensorMath.addmm(gradA, ev.zero, gradA, ev.one, gradOutput, mb)
    SparseTensorMath.addmm(gradB, ev.zero, gradB, ev.one, ma, gradOutput)

    gradInput
  }

  override def toString: String = s"SparseMM()"

  override def canEqual(other: Any): Boolean = other.isInstanceOf[MM[T]]

  override def equals(other: Any): Boolean = other match {
    case that: SparseMM[T] =>
      super.equals(that) &&
        (that canEqual this)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode())
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def clearState(): SparseMM.this.type = {
    super.clearState()

    gradInput[Tensor[T]](1).set()
    gradInput[Tensor[T]](2).set()

    this
  }
}

object SparseMM {
  def apply[@specialized(Float, Double) T: ClassTag]
  ()(implicit ev: TensorNumeric[T]): SparseMM[T] = {
    new SparseMM[T]()
  }
}
