package com.tencent.angel.algorithm.aggregator

import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.tencent.angel.algorithm.aggregator.AggregatorType.AggregatorType

import scala.reflect.ClassTag

object AggregatorType extends Enumeration {
  type AggregatorType = Value
  val GCN: Value = Value("GCN")
  val Mean: Value = Value("Mean")
  val MeanPool: Value = Value("MeanPool")
  val MaxPool: Value = Value("MaxPool")
}

trait Aggregator[T] {
  def aggregate(input: ModuleNode[T], neighbor: ModuleNode[T]): ModuleNode[T]
}

object Aggregator {
  def apply[T: ClassTag](aggregatorType: AggregatorType,
               dim: Int,
               activation: String,
               concat: Boolean)(implicit ev: TensorNumeric[T]): Aggregator[T] = {
    aggregatorType match {
      case AggregatorType.GCN => new GCNAggregator[T](dim, activation)
      case AggregatorType.Mean => new MeanAggregator[T](dim, activation, concat)
      case AggregatorType.MeanPool => new MeanPoolAggregator[T](dim, activation)
      case AggregatorType.MaxPool => new MaxPoolAggregator[T](dim, activation)
    }
  }
}