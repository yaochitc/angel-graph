package com.tencent.angel.graph.ops

import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.data.graph.EdgeId

import scala.collection.mutable.ArrayBuffer

object FeatureOps {
  def getDenseFeature(nodes: Array[Long],
                      featureIds: Array[Int],
                      dimensions: Array[Int])(implicit graph: IGraph): Array[Array[Float]] = {
    val featureLen = dimensions.sum
    val features = graph.getNodeFloatFeature(nodes, featureIds)

    nodes.map(node => {
      val nodeFeatures = features.get(node)

      val featureValues = Array.ofDim[Float](featureLen)
      var offset = 0
      for ((featureSize, dimension) <- nodeFeatures.getFeatureSizes.zip(dimensions)) {
        for (featureOffset <- offset until offset + Math.min(featureSize, dimension)) {
          featureValues(featureOffset) = nodeFeatures.getFeatureValues()(featureOffset)
        }
        offset += dimension
      }
      featureValues
    })
  }

  def getEdgeDenseFeature(edges: Array[EdgeId],
                          featureIds: Array[Int],
                          dimensions: Array[Int])(implicit graph: IGraph) = {
    val featureLen = dimensions.sum
    val features = graph.getEdgeFloatFeature(edges, featureIds)

    edges.map(edge => {
      val edgeFeatures = features.get(edge)

      val featureValues = Array.ofDim[Float](featureLen)
      var offset = 0
      for ((featureSize, dimension) <- edgeFeatures.getFeatureSizes.zip(dimensions)) {
        for (featureOffset <- offset until offset + Math.min(featureSize, dimension)) {
          featureValues(featureOffset) = edgeFeatures.getFeatureValues()(featureOffset)
        }
        offset += dimension
      }
      featureValues
    })
  }

  def getSparseFeature(nodes: Array[Long],
                       featureIds: Array[Int])(implicit graph: IGraph): Array[(Array[(Int, Int)], Array[Long], (Int, Int))] = {
    val features = graph.getNodeLongFeature(nodes, featureIds)

    nodes.map(node => {
      val nodeFeatures = features.get(node)

      val values = ArrayBuffer[Long]()
      val indices = ArrayBuffer[(Int, Int)]()

      var offset = 0
      var maxFeatureLen = 0
      for ((featureSize, featureIdx) <- nodeFeatures.getFeatureSizes.zipWithIndex) {
        for (featureOffset <- 0 until featureSize) {
          indices += Tuple2(featureIdx, featureOffset)
          values += nodeFeatures.getFeatureValues()(offset + featureOffset)
        }
        maxFeatureLen = Math.max(maxFeatureLen, featureSize)
        offset += featureSize
      }
      (indices.toArray, values.toArray, (featureIds.length, maxFeatureLen))
    })
  }

  def getEdgeSparseFeature(edges: Array[EdgeId],
                           featureIds: Array[Int])(implicit graph: IGraph): Array[(Array[(Int, Int)], Array[Long], (Int, Int))] = {
    val features = graph.getEdgeLongFeature(edges, featureIds)

    edges.map(edge => {
      val edgeFeatures = features.get(edge)

      val values = ArrayBuffer[Long]()
      val indices = ArrayBuffer[(Int, Int)]()

      var offset = 0
      var maxFeatureLen = 0
      for ((featureSize, featureIdx) <- edgeFeatures.getFeatureSizes.zipWithIndex) {
        for (featureOffset <- 0 until featureSize) {
          indices += Tuple2(featureIdx, featureOffset)
          values += edgeFeatures.getFeatureValues()(offset + featureOffset)
        }
        maxFeatureLen = Math.max(maxFeatureLen, featureSize)
        offset += featureSize
      }
      (indices.toArray, values.toArray, (featureIds.length, maxFeatureLen))
    })
  }
}
