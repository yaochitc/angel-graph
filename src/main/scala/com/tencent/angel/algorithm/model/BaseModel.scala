package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.graph.client.IGraph

trait BaseModel[T] {
  def sample(input: Array[Long], graph: IGraph): Array[Sample[T]]

  def buildModel(): Model[T]
}
