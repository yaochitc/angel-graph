package com.tencent.angel.graph.ops

import com.tencent.angel.graph.client.IGraph

import scala.collection.mutable.ArrayBuffer

object NeighborOps {
  def sampleNeighbor(nodes: Array[Long],
                     edgeTypes: Array[Int],
                     count: Int,
                     defaultNode: Long = -1)(implicit graph: IGraph):
  (Array[Array[Long]], Array[Array[Float]], Array[Array[Int]]) = {
    val batchNeighbor = graph.sampleNeighbor(nodes, edgeTypes, count)

    val neighborIds = Array.ofDim[Long](nodes.length, count)
    val neighborWeights = Array.ofDim[Float](nodes.length, count)
    val neighborEdgeTypes = Array.ofDim[Int](nodes.length, count)

    nodes.zipWithIndex.foreach { case (node, index) => {
      val startIndex = count * index
      val neighbor = batchNeighbor.get(node)
      val (neighborIdArr, neighborWeightArr, neighborEdgeTypeArr) =
        neighbor.size match {
          case 0 =>
            (Array.fill[Long](count)(defaultNode),
              Array.fill[Float](count)(0f),
              Array.fill[Int](count)(-1))
          case _ =>
            (neighbor.getNodeIds,
              neighbor.getNodeWeights,
              neighbor.getEdgeTypes)
        }

      neighborIds(index) = neighborIdArr
      neighborWeights(index) = neighborWeightArr
      neighborEdgeTypes(index) = neighborEdgeTypeArr
    }
    }

    (neighborIds, neighborWeights, neighborEdgeTypes)
  }

  def sampleFanout(nodes: Array[Long],
                   edgeTypes: Array[Array[Int]],
                   counts: Array[Int],
                   defaultNode: Long = -1)(implicit graph: IGraph):
  (Array[Array[Long]], Array[Array[Float]], Array[Array[Int]]) = {
    val neighborIdsBuffer = ArrayBuffer[Array[Long]]()
    val neighborWeightsBuffer = ArrayBuffer[Array[Float]]()
    val neighborEdgeTypesBuffer = ArrayBuffer[Array[Int]]()

    neighborIdsBuffer += nodes

    var lastNeighborIds = nodes
    for ((hopEdgeTypes, count) <- edgeTypes zip counts) {
      val (neighborIds, neighborWeights, neighborEdgeTypes) = sampleNeighbor(lastNeighborIds, hopEdgeTypes, count)
      lastNeighborIds = neighborIds.flatten

      neighborIdsBuffer += lastNeighborIds
      neighborWeightsBuffer += neighborWeights.flatten
      neighborEdgeTypesBuffer += neighborEdgeTypes.flatten
    }
    (neighborIdsBuffer.toArray, neighborWeightsBuffer.toArray, neighborEdgeTypesBuffer.toArray)
  }

  def getMultiHopNeighbor(nodes: Array[Long],
                          edgeTypes: Array[Array[Int]])(implicit graph: IGraph):
  (Array[Array[Long]], Array[(Array[(Int, Int)], Array[Float], (Int, Int))]) = {
    val neighborIdsBuffer = ArrayBuffer[Array[Long]]()
    val adjMatrixBuffer = ArrayBuffer[(Array[(Int, Int)], Array[Float], (Int, Int))]()

    neighborIdsBuffer += nodes

    var lastNeighborIds = nodes
    for (hopEdgeTypes <- edgeTypes) {
      val neighbor = graph.getFullNeighbor(lastNeighborIds, hopEdgeTypes)
      val nextNeighborIds = lastNeighborIds.flatMap(neighbor.get(_).getNodeIds).distinct

      val neighborId2Indices = nextNeighborIds.indices.map(i => (nextNeighborIds(i), i)).toMap
      val adjIndices = nodes.indices.flatMap(i => neighbor.get(nodes(i)).getNodeIds.map(nodeId => (i, neighborId2Indices(nodeId)))).toArray
      val adjValues = nodes.indices.flatMap(i => neighbor.get(nodes(i)).getNodeWeights).toArray
      val adjShape = (lastNeighborIds.length, nextNeighborIds.length)
      val adjMatrix = (adjIndices, adjValues, adjShape)

      neighborIdsBuffer += nextNeighborIds
      adjMatrixBuffer += adjMatrix
      lastNeighborIds = nextNeighborIds
    }

    (neighborIdsBuffer.toArray, adjMatrixBuffer.toArray)
  }
}
