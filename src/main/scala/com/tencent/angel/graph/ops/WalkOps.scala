package com.tencent.angel.graph.ops

import java.lang.{Long => JLong}

import com.tencent.angel.graph.client.{CompactSampler, IGraph}

object WalkOps {
  def randomWalk(nodes: Array[Long],
                 edgeTypes: Array[Int],
                 walkLen: Int,
                 p: Float,
                 q: Float,
                 defaultNode: Long = -1)(implicit graph: IGraph): Array[Array[Long]] = {
    val paths = Array.ofDim[Long](nodes.length, walkLen + 1)
    val batchFullNeighbor = graph.getSortedFullNeighbor(nodes, edgeTypes)

    val firstWalkNodes = nodes.map(node => {
      val neighbor = batchFullNeighbor.get(node)
      neighbor.size match {
        case 0 => defaultNode
        case _ =>
          val neighborNodeIds = neighbor.getNodeIds.map(long2Long)
          val neighborNodeWeights = neighbor.getNodeWeights
          val sampler = new CompactSampler[JLong]
          sampler.init(neighborNodeIds, neighborNodeWeights)
          Long2long(sampler.sample())
      }
    })

    for ((nodeId, idx) <- nodes.zipWithIndex) {
      paths(idx)(0) = nodeId
    }

    for ((nodeId, idx) <- firstWalkNodes.zipWithIndex) {
      paths(idx)(1) = nodeId
    }

    var childWalkNodes = firstWalkNodes
    var parentNodeNeighbors = batchFullNeighbor
    var walkedLen = 1
    while (walkedLen < walkLen) {
      val nodeWalkPair = graph.biasedSampleNeighbor(childWalkNodes, parentNodeNeighbors, edgeTypes, 1, p, q)
      val walkNodes = childWalkNodes.map(node => {
        val neighborNodeIds = nodeWalkPair.getNodeNeighborIds.get(node)
        neighborNodeIds.length match {
          case 0 => defaultNode
          case _ => neighborNodeIds(0)
        }
      })

      for ((nodeId, idx) <- walkNodes.zipWithIndex) {
        paths(idx)(walkedLen + 1) = nodeId
      }

      childWalkNodes = walkNodes
      parentNodeNeighbors = nodeWalkPair.getParentNodeNeighbors
      walkedLen += 1
    }

    paths
  }

  def genPair(paths: Array[Array[Long]],
              pathLen: Int,
              numPairs: Int,
              leftWinSize: Int,
              rightWinSize: Int): (Array[Array[Long]], Array[Array[Long]]) = {
    val batchSize = paths.length

    val src = Array.ofDim[Long](batchSize, numPairs)
    val tar = Array.ofDim[Long](batchSize, numPairs)

    for (i <- 0 until batchSize) {
      var c = 0
      for (j <- 0 until pathLen) {
        var k = 0
        while ((j - k - 1) >= 0 && k < leftWinSize) {
          src(i)(c) = paths(i)(j)
          tar(i)(c) = paths(i)(j - k - 1)
          k += 1
          c += 1
        }

        k = 0
        while ((j + k + 1) < pathLen && k < rightWinSize) {
          src(i)(c) = paths(i)(j)
          tar(i)(c) = paths(i)(j + k + 1)
          k += 1
          c += 1
        }
      }
    }
    (src, tar)
  }
}
