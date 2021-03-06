package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.graph.Edge;
import com.tencent.angel.graph.data.graph.Node;
import com.tencent.angel.graph.data.NodeEdgesPair;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class PartInitNeighborParam extends PartitionUpdateParam {
	private NodeEdgesPair[] nodeEdgesPairs;

	/**
	 * Store position: start index in nodeIds
	 */
	private int startIndex;

	/**
	 * Store position: end index in nodeIds
	 */
	private int endIndex;

	public PartInitNeighborParam(int matrixId, PartitionKey partKey, NodeEdgesPair[] nodeEdgesPairs
					, int startIndex, int endIndex) {
		super(matrixId, partKey);
		this.nodeEdgesPairs = nodeEdgesPairs;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}

	public PartInitNeighborParam() {
		this(0, null, null, 0, 0);
	}

	public NodeEdgesPair[] getNodeEdgesPairs() {
		return nodeEdgesPairs;
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);

		buf.writeInt(endIndex - startIndex);

		for (int i = startIndex; i < endIndex; i++) {
			NodeEdgesPair nodeEdgesPair = nodeEdgesPairs[i];
			nodeEdgesPair.getNode().serialize(buf);

			Edge[] edges = nodeEdgesPair.getEdges();
			int numEdges = edges.length;
			buf.writeInt(numEdges);

			for (Edge edge : edges) {
				edge.serialize(buf);
			}
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);

		int nodeEdgesNum = buf.readInt();

		nodeEdgesPairs = new NodeEdgesPair[nodeEdgesNum];
		for (int i = 0; i < nodeEdgesNum; i++) {
			Node node = new Node();
			node.deserialize(buf);

			int numEdges = buf.readInt();
			Edge[] edges = new Edge[numEdges];

			for (int j = 0; j < numEdges; j++) {
				Edge edge = new Edge();
				edge.deserialize(buf);
				edges[j] = edge;
			}
			nodeEdgesPairs[i] = new NodeEdgesPair(node, edges);
		}
	}

	@Override
	public int bufferLen() {
		int len = super.bufferLen();
		len += 4;
		for (NodeEdgesPair nodeEdgesPair : nodeEdgesPairs) {
			len += nodeEdgesPair.getNode().bufferLen();

			len += 4;
			for (Edge edge : nodeEdgesPair.getEdges()) {
				len += edge.bufferLen();
			}
		}
		return len;
	}
}
