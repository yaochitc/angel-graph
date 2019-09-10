package com.tencent.angel.graph;

import com.tencent.angel.graph.client.IGraph;
import com.tencent.angel.graph.client.PSGraph;
import com.tencent.angel.graph.data.NodeEdgesPair;
import com.tencent.angel.graph.data.NodeIDWeightPairs;
import com.tencent.angel.graph.data.graph.Edge;
import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.graph.data.graph.Node;
import com.tencent.angel.spark.context.PSContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class InitNeighborTest {
	private IGraph psGraph;

	@Before
	public void setup() throws Exception {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("app");
		sparkConf.setMaster("local");
		PSContext.getOrCreate(SparkContext.getOrCreate(sparkConf));
		psGraph = PSGraph.create(0, 1000);
	}

	@Test
	public void testHomoGraph() throws Exception {
		int nodeType = 0;
		int[] edgeTypes = new int[1];
		float[] edgeAccSumWeights = new float[1];
		edgeAccSumWeights[0] = 1.0f;
		float edgeTotalSumWeights = 1.0f;

		NodeEdgesPair[] nodeEdgesPairs = new NodeEdgesPair[3];

		long[] neighborsNode1 = new long[2];
		neighborsNode1[0] = 2;
		neighborsNode1[1] = 3;

		int[] neighborGroupIndicesNode1 = new int[1];
		neighborGroupIndicesNode1[0] = neighborsNode1.length;

		float[] neighborWeightsNode1 = new float[2];
		neighborWeightsNode1[0] = 1.0f;
		neighborWeightsNode1[1] = 3.0f;

		Node node1 = createNode(1, nodeType, 1.0f,
				edgeTypes, edgeAccSumWeights, edgeTotalSumWeights,
				neighborGroupIndicesNode1, neighborsNode1, neighborWeightsNode1);

		Edge[] edges1 = new Edge[2];
		edges1[0] = createEdge(new EdgeId(1, 2, 0), 0, 1.0f);
		edges1[1] = createEdge(new EdgeId(1, 3, 0), 0, 1.0f);

		nodeEdgesPairs[0] = new NodeEdgesPair(node1, edges1);

		Node node2 = createNode(2, nodeType, 1.0f);

		nodeEdgesPairs[1] = new NodeEdgesPair(node2, new Edge[0]);

		Node node3 = createNode(3, nodeType, 1.0f);

		nodeEdgesPairs[2] = new NodeEdgesPair(node3, new Edge[0]);

		psGraph.initNeighbor(nodeEdgesPairs);
		psGraph.buildGlobalSampler();

		long[] nodeIds = new long[1];
		nodeIds[0] = 1;
		Map<Long, NodeIDWeightPairs> id2Neighbors1 = psGraph.sampleNeighbor(nodeIds, edgeTypes, 1);
		Map<Long, NodeIDWeightPairs> id2Neighbors2 = psGraph.getFullNeighbor(nodeIds, edgeTypes);
		Map<Long, NodeIDWeightPairs> id2Neighbors3 = psGraph.getSortedFullNeighbor(nodeIds, edgeTypes);
		Map<Long, NodeIDWeightPairs> id2Neighbors4 = psGraph.getTopKNeighbor(nodeIds, edgeTypes, 2);

		long[] sampleNodes = psGraph.sampleNode(nodeType, 5);

		EdgeId[] edges = psGraph.sampleEdge(0, 5);
	}

	private Node createNode(long id, int type, float weight) {
		return createNode(id, type, weight,
				new int[0], new float[0], 0f,
				new int[0], new long[0], new float[0]);
	}

	private Node createNode(long id, int type, float weight, int[] edgeTypes, float[] edgeAccSumWeights,
							float edgeTotalSumWeights, int[] neigborGroupIndices, long[] neighbors, float[] neighborAccSumWeights) {
		return new Node(id, type, weight, edgeTypes, edgeAccSumWeights, edgeTotalSumWeights,
				neigborGroupIndices, neighbors, neighborAccSumWeights,
				new int[0], new long[0],
				new int[0], new float[0],
				new int[0], new byte[0]
		);
	}

	private Edge createEdge(EdgeId id, int type, float weight) {
		return new Edge(id, type, weight,
				new int[0], new long[0],
				new int[0], new float[0],
				new int[0], new byte[0]);
	}
}
