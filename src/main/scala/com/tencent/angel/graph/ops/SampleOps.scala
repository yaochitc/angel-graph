package com.tencent.angel.graph.ops

import com.tencent.angel.graph.client.IGraph

object SampleOps {
  def sampleNode(nodeType: Int,
                 n: Int)(implicit graph: IGraph): Array[Long] = {
    graph.sampleNode(nodeType, n)
  }

  def sampleNodeWithSrc(srcNodes: Array[Long],
                        n: Int)(implicit graph: IGraph): Array[Array[Long]] = {
    val nodeTypes = graph.getNodeType(srcNodes)
    val nodeType2Nodes = srcNodes.groupBy(node => nodeTypes.get(node))
    val nodeType2Count = nodeType2Nodes
      .mapValues(_.length)

    val node2Samples = nodeType2Count.flatMap { case (nodeType, count) => {
      val sampeNodes = graph.sampleNode(nodeType, count * n)
      val nodes = nodeType2Nodes(nodeType)

      nodes.indices.map(i => {
        (nodes(i), sampeNodes.slice(i * n, (i + 1) * n))
      })
    }
    }

    srcNodes.map(node => node2Samples(node))
  }
}
