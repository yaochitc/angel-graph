package com.tencent.angel.graph.ps.storage.vector;

import com.tencent.angel.graph.client.FastSampler;
import com.tencent.angel.graph.client.Sampler;
import com.tencent.angel.graph.data.graph.Edge;
import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.graph.data.graph.Node;
import com.tencent.angel.ps.storage.vector.ServerComplexTypeRow;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GraphServerRow extends ServerComplexTypeRow {
	private final Map<Long, Node> id2Nodes;
	private final Map<EdgeId, Edge> id2Edges;
	private final Map<Integer, Float> nodeWeightSums;
	private final Map<Integer, Float> edgeWeightSums;

	private Map<Integer, Sampler<Long>> nodeSamplers;
	private Map<Integer, Sampler<EdgeId>> edgeSamplers;

	public GraphServerRow(long startCol, long endCol) {
		super(null, 0, null, startCol, endCol, 0, null);
		id2Nodes = new Long2ObjectOpenHashMap<>();
		id2Edges = new Object2ObjectOpenHashMap<>();
		nodeWeightSums = new Int2FloatOpenHashMap();
		edgeWeightSums = new Int2FloatOpenHashMap();
	}

	@Override
	public void init() {
	}

	public void addNode(Node node) {
		id2Nodes.put(node.getId(), node);
		Float weightSum = nodeWeightSums.getOrDefault(node.getType(), 0f);
		nodeWeightSums.put(node.getType(), weightSum + node.getWeight());
	}

	public void addEdge(Edge edge) {
		id2Edges.put(edge.getId(), edge);
		Float weightSum = edgeWeightSums.getOrDefault(edge.getType(), 0f);
		edgeWeightSums.put(edge.getType(), weightSum + edge.getWeight());
	}

	public Node getNode(long id) {
		return id2Nodes.get(id);
	}

	public Edge getEdge(EdgeId id) {
		return id2Edges.get(id);
	}

	public void buildNodeSampler() {
		Map<Integer, List<Node>> nodeType2Nodes = new Int2ObjectOpenHashMap<>();
		for (Node node : id2Nodes.values()) {
			int nodeType = node.getType();
			List<Node> nodes = nodeType2Nodes.computeIfAbsent(nodeType, k -> new ArrayList<>());
			nodes.add(node);
		}

		nodeSamplers = new Int2ObjectOpenHashMap<>();
		for (Map.Entry<Integer, List<Node>> entry : nodeType2Nodes.entrySet()) {
			int nodeType = entry.getKey();
			float weightSum = nodeWeightSums.get(nodeType);

			List<Node> nodes = entry.getValue();
			Long[] nodeIds = new Long[nodes.size()];
			float[] normWeights = new float[nodes.size()];
			for (int i = 0; i < nodes.size(); i++) {
				Node node = nodes.get(i);
				nodeIds[i] = node.getId();
				normWeights[i] = node.getWeight() / weightSum;
			}

			FastSampler<Long> nodeSampler = new FastSampler<>();
			nodeSampler.init(nodeIds, normWeights);
			nodeSamplers.put(nodeType, nodeSampler);
		}
	}

	public void buildEdgeSampler() {
		Map<Integer, List<Edge>> edgeType2Edges = new Int2ObjectOpenHashMap<>();
		for (Edge edge : id2Edges.values()) {
			int edgeType = edge.getType();
			List<Edge> edges = edgeType2Edges.computeIfAbsent(edgeType, k -> new ArrayList<>());
			edges.add(edge);
		}

		edgeSamplers = new Int2ObjectOpenHashMap<>();
		for (Map.Entry<Integer, List<Edge>> entry : edgeType2Edges.entrySet()) {
			int nodeType = entry.getKey();
			float weightSum = edgeWeightSums.get(nodeType);

			List<Edge> edges = entry.getValue();
			EdgeId[] edgeIds = new EdgeId[edges.size()];
			float[] normWeights = new float[edges.size()];
			for (int i = 0; i < edges.size(); i++) {
				Edge edge = edges.get(i);
				edgeIds[i] = edge.getId();
				normWeights[i] = edge.getWeight() / weightSum;
			}

			FastSampler<EdgeId> edgeSampler = new FastSampler<>();
			edgeSampler.init(edgeIds, normWeights);
			edgeSamplers.put(nodeType, edgeSampler);
		}
	}

	public Map<Integer, Float> getNodeWeightSums() {
		return nodeWeightSums;
	}

	public Map<Integer, Float> getEdgeWeightSums() {
		return edgeWeightSums;
	}

	public long[] sampleNode(int nodeType, int count) {
		long[] nodeIds = new long[count];
		for (int i = 0; i < count; i++) {
			nodeIds[i] = nodeSamplers.get(nodeType).sample();
		}
		return nodeIds;
	}

	public EdgeId[] sampleEdge(int edgeType, int count) {
		EdgeId[] edgeIds = new EdgeId[count];
		for (int i = 0; i < count; i++) {
			edgeIds[i] = edgeSamplers.get(edgeType).sample();
		}
		return edgeIds;
	}

	@Override
	public Object deepClone() {
		GraphServerRow clone = new GraphServerRow(startCol, endCol);
		for (Map.Entry<Long, Node> entry : id2Nodes.entrySet()) {
			clone.addNode((Node) entry.getValue().deepClone());
		}

		for (Map.Entry<EdgeId, Edge> entry : id2Edges.entrySet()) {
			clone.addEdge((Edge) entry.getValue().deepClone());
		}

		return clone;
	}

	@Override
	public Object adaptiveClone() {
		return this;
	}
}
