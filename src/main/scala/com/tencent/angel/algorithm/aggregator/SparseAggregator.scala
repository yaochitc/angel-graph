package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.tencent.angel.algorithm.aggregator.SparseAggregatorType.SparseAggregatorType

import scala.reflect.ClassTag

object SparseAggregatorType extends Enumeration {
  type SparseAggregatorType = Value
  val GCN: Value = Value("GCN")
  val Mean: Value = Value("Mean")
}

trait SparseAggregator[T] {
  def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T], adj: ModuleNode[T]): ModuleNode[T]
}

object SparseAggregator {
  def apply[T: ClassTag](aggregatorType: SparseAggregatorType,
                         dim: Int,
                         activation: String)(implicit ev: TensorNumeric[T]): SparseAggregator[T] = {
    aggregatorType match {
      case SparseAggregatorType.GCN => new GCNSparseAggregator[T](dim, activation)
      case SparseAggregatorType.Mean => new MeanSparseAggregator[T](dim, activation)
    }
  }
}