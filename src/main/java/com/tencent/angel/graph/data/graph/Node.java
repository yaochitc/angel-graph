package com.tencent.angel.graph.data.graph;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Graph node
 */
public class Node implements IElement {

	/**
	 * Node id
	 */
	private long id;

	/**
	 * Node type
	 */
	private int type;

	/**
	 * Node weight
	 */
	private float weight;

	/**
	 * Edge types
	 */
	private int[] edgeTypes;

	/**
	 * Accumulation sums of edge weights, dimension is equal to edgeTypes
	 */
	private float[] edgeAccSumWeights;

	/**
	 * Total sum of edge weights
	 */
	private float edgeTotalSumWeights;

	/**
	 * Neighbor group positions in "neighbors", each neighbor group corresponding to an edge type
	 */
	private int[] neigborGroupIndices;

	/**
	 * Neighbor node ids of this node
	 */
	private long[] neighbors;

	/**
	 * Accumulation sums of neighbor node weights
	 */
	private float[] neighborAccSumWeights;

	/**
	 * Long type feature indices
	 */
	private int[] longFeatureIndices;

	/**
	 * Long feature values
	 */
	private long[] longFeatures;

	/**
	 * Float feature indices
	 */
	private int[] floatFeatureIndices;

	/**
	 * Float feature values
	 */
	private float[] floatFeatures;

	/**
	 * Binary feature indices
	 */
	private int[] binaryFeatureIndices;

	/**
	 * Binary feature values
	 */
	private byte[] binaryFeatures;

	public Node() {
	}

	public Node(long id, int type, float weight, int[] edgeTypes, float[] edgeAccSumWeights,
				float edgeTotalSumWeights, int[] neigborGroupIndices, long[] neighbors, float[] neighborAccSumWeights,
				int[] longFeatureIndices, long[] longFeatures, int[] floatFeatureIndices,
				float[] floatFeatures,
				int[] binaryFeatureIndices, byte[] binaryFeatures) {
		this.id = id;
		this.type = type;
		this.weight = weight;
		this.edgeTypes = edgeTypes;
		this.edgeAccSumWeights = edgeAccSumWeights;
		this.edgeTotalSumWeights = edgeTotalSumWeights;
		this.neigborGroupIndices = neigborGroupIndices;
		this.neighbors = neighbors;
		this.neighborAccSumWeights = neighborAccSumWeights;
		this.longFeatureIndices = longFeatureIndices;
		this.longFeatures = longFeatures;
		this.floatFeatureIndices = floatFeatureIndices;
		this.floatFeatures = floatFeatures;
		this.binaryFeatureIndices = binaryFeatureIndices;
		this.binaryFeatures = binaryFeatures;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public float getWeight() {
		return weight;
	}

	public void setWeight(float weight) {
		this.weight = weight;
	}

	public float[] getEdgeAccSumWeights() {
		return edgeAccSumWeights;
	}

	public void setEdgeAccSumWeights(float[] edgeAccSumWeights) {
		this.edgeAccSumWeights = edgeAccSumWeights;
	}

	public float getEdgeTotalSumWeights() {
		return edgeTotalSumWeights;
	}

	public void setEdgeTotalSumWeights(float edgeTotalSumWeights) {
		this.edgeTotalSumWeights = edgeTotalSumWeights;
	}

	public int[] getNeigborGroupIndices() {
		return neigborGroupIndices;
	}

	public void setNeigborGroupIndices(int[] neigborGroupIndices) {
		this.neigborGroupIndices = neigborGroupIndices;
	}

	public long[] getNeighbors() {
		return neighbors;
	}

	public void setNeighbors(long[] neighbors) {
		this.neighbors = neighbors;
	}

	public void setNeighborAccSumWeights(float[] neighborAccSumWeights) {
		this.neighborAccSumWeights = neighborAccSumWeights;
	}

	public float[] getNeighborAccSumWeights() {
		return neighborAccSumWeights;
	}

	public int[] getLongFeatureIndices() {
		return longFeatureIndices;
	}

	public void setLongFeatureIndices(int[] longFeatureIndices) {
		this.longFeatureIndices = longFeatureIndices;
	}

	public int[] getEdgeTypes() {
		return edgeTypes;
	}

	public void setEdgeTypes(int[] edgeTypes) {
		this.edgeTypes = edgeTypes;
	}

	public long[] getLongFeatures() {
		return longFeatures;
	}

	public void setLongFeatures(long[] longFeatures) {
		this.longFeatures = longFeatures;
	}

	public int[] getFloatFeatureIndices() {
		return floatFeatureIndices;
	}

	public void setFloatFeatureIndices(int[] floatFeatureIndices) {
		this.floatFeatureIndices = floatFeatureIndices;
	}

	public float[] getFloatFeatures() {
		return floatFeatures;
	}

	public void setFloatFeatures(float[] floatFeatures) {
		this.floatFeatures = floatFeatures;
	}

	public int[] getBinaryFeatureIndices() {
		return binaryFeatureIndices;
	}

	public void setBinaryFeatureIndices(int[] binaryFeatureIndices) {
		this.binaryFeatureIndices = binaryFeatureIndices;
	}

	public byte[] getBinaryFeatures() {
		return binaryFeatures;
	}

	public void setBinaryFeatures(byte[] binaryFeatures) {
		this.binaryFeatures = binaryFeatures;
	}

	@Override
	public Object deepClone() {
		int[] edgeTypeClone = Arrays.copyOf(edgeTypes, edgeTypes.length);
		float[] edgeAccSumWeightsClone = Arrays.copyOf(edgeAccSumWeights, edgeAccSumWeights.length);
		int[] neigborGroupIndicesClone = Arrays.copyOf(neigborGroupIndices, neigborGroupIndices.length);
		long[] neighborsClone = Arrays.copyOf(neighbors, neighbors.length);
		float[] neighborAccSumWeightsClone = Arrays.copyOf(neighborAccSumWeights, neighborAccSumWeights.length);
		int[] longFeatureIndicesClone = Arrays.copyOf(longFeatureIndices, longFeatureIndices.length);
		long[] longFeaturesClone = Arrays.copyOf(longFeatures, longFeatures.length);
		int[] floatFeatureIndicesClone = Arrays.copyOf(floatFeatureIndices, floatFeatureIndices.length);
		float[] floatFeaturesClone = Arrays.copyOf(floatFeatures, floatFeatures.length);
		int[] binaryFeatureIndicesClone = Arrays.copyOf(binaryFeatureIndices, binaryFeatureIndices.length);
		byte[] binaryFeaturesClone = Arrays.copyOf(binaryFeatures, binaryFeatures.length);

		return new Node(id, type, weight, edgeTypeClone, edgeAccSumWeightsClone,
				edgeTotalSumWeights, neigborGroupIndicesClone,
				neighborsClone, neighborAccSumWeightsClone,
				longFeatureIndicesClone, longFeaturesClone, floatFeatureIndicesClone, floatFeaturesClone,
				binaryFeatureIndicesClone, binaryFeaturesClone);
	}

	@Override
	public void serialize(ByteBuf buf) {
		// Node id
		buf.writeLong(id);

		// Node type
		buf.writeInt(type);

		// Node weight
		buf.writeFloat(weight);

		// Node edge type num
		buf.writeInt(edgeTypes.length);

		for (float edgeAccSumWeight : edgeAccSumWeights) {
			buf.writeFloat(edgeAccSumWeight);
		}

		buf.writeFloat(edgeTotalSumWeights);

		for (int neigborGroupIndex : neigborGroupIndices) {
			buf.writeInt(neigborGroupIndex);
		}

		buf.writeInt(neighbors.length);

		for (long neighbor : neighbors) {
			buf.writeLong(neighbor);
		}

		for (float neighborAccSumWeight : neighborAccSumWeights) {
			buf.writeFloat(neighborAccSumWeight);
		}

		buf.writeInt(longFeatureIndices.length);

		for (int longFeatureIndex : longFeatureIndices) {
			buf.writeInt(longFeatureIndex);
		}

		buf.writeInt(longFeatures.length);

		for (long longFeature : longFeatures) {
			buf.writeLong(longFeature);
		}

		buf.writeInt(floatFeatureIndices.length);

		for (int floatFeatureIndex : floatFeatureIndices) {
			buf.writeInt(floatFeatureIndex);
		}

		buf.writeInt(floatFeatures.length);

		for (float floatFeature : floatFeatures) {
			buf.writeFloat(floatFeature);
		}

		buf.writeInt(binaryFeatureIndices.length);

		for (int binaryFeatureIndex : binaryFeatureIndices) {
			buf.writeInt(binaryFeatureIndex);
		}

		buf.writeInt(binaryFeatures.length);

		buf.writeBytes(binaryFeatures);
	}

	@Override
	public void deserialize(ByteBuf buf) {
		// Node id
		id = buf.readLong();

		// Node type
		type = buf.readInt();

		// Node weight
		weight = buf.readFloat();

		int edgeTypeNum = buf.readInt();

		edgeTypes = new int[edgeTypeNum];
		for (int i = 0; i < edgeTypeNum; i++) {
			edgeTypes[i] = i;
		}

		edgeAccSumWeights = new float[edgeTypeNum];
		for (int i = 0; i < edgeTypeNum; i++) {
			edgeAccSumWeights[i] = buf.readFloat();
		}

		edgeTotalSumWeights = buf.readFloat();

		neigborGroupIndices = new int[edgeTypeNum];
		for (int i = 0; i < edgeTypeNum; i++) {
			neigborGroupIndices[i] = buf.readInt();
		}

		int neighborsNum = buf.readInt();

		neighbors = new long[neighborsNum];
		for (int i = 0; i < neighborsNum; i++) {
			neighbors[i] = buf.readLong();
		}

		neighborAccSumWeights = new float[neighborsNum];
		for (int i = 0; i < neighborsNum; i++) {
			neighborAccSumWeights[i] = buf.readFloat();
		}

		int longFeatureNum = buf.readInt();

		longFeatureIndices = new int[longFeatureNum];
		for (int i = 0; i < longFeatureNum; i++) {
			longFeatureIndices[i] = buf.readInt();
		}

		int longFeatureValueNum = buf.readInt();

		longFeatures = new long[longFeatureValueNum];
		for (int i = 0; i < longFeatureValueNum; i++) {
			longFeatures[i] = buf.readLong();
		}

		int floatFeatureNum = buf.readInt();

		floatFeatureIndices = new int[floatFeatureNum];
		for (int i = 0; i < floatFeatureNum; i++) {
			floatFeatureIndices[i] = buf.readInt();
		}

		int floatFeatureValueNum = buf.readInt();

		floatFeatures = new float[floatFeatureValueNum];
		for (int i = 0; i < floatFeatureValueNum; i++) {
			floatFeatures[i] = buf.readFloat();
		}

		int binaryFeatureNum = buf.readInt();

		binaryFeatureIndices = new int[binaryFeatureNum];
		for (int i = 0; i < binaryFeatureNum; i++) {
			binaryFeatureIndices[i] = buf.readInt();
		}

		int binaryFeatureValueNum = buf.readInt();
		binaryFeatures = buf.readBytes(binaryFeatureValueNum).array();
	}

	@Override
	public int bufferLen() {
		return 16 + (8 + edgeTypes.length * 4) +
				(4 + edgeTypes.length * 4 + neighbors.length * 8 + neighborAccSumWeights.length * 4) +
				(8 + longFeatureIndices.length * 4 + longFeatures.length * 8) +
				(8 + floatFeatureIndices.length * 4 + floatFeatures.length * 4) +
				(8 + binaryFeatureIndices.length * 4 + binaryFeatures.length);
	}

	@Override
	public void serialize(DataOutputStream dataOutputStream) throws IOException {
		// Node id
		dataOutputStream.writeLong(id);

		// Node type
		dataOutputStream.writeInt(type);

		// Node weight
		dataOutputStream.writeFloat(weight);

		// Node edge type num
		dataOutputStream.writeInt(edgeTypes.length);

		for (float edgeAccSumWeight : edgeAccSumWeights) {
			dataOutputStream.writeFloat(edgeAccSumWeight);
		}

		dataOutputStream.writeFloat(edgeTotalSumWeights);

		for (int neigborGroupIndex : neigborGroupIndices) {
			dataOutputStream.writeInt(neigborGroupIndex);
		}

		dataOutputStream.writeInt(neighbors.length);

		for (long neighbor : neighbors) {
			dataOutputStream.writeLong(neighbor);
		}

		for (float neighborAccSumWeight : neighborAccSumWeights) {
			dataOutputStream.writeFloat(neighborAccSumWeight);
		}

		dataOutputStream.writeInt(longFeatureIndices.length);

		for (int longFeatureIndex : longFeatureIndices) {
			dataOutputStream.writeInt(longFeatureIndex);
		}

		dataOutputStream.writeInt(longFeatures.length);

		for (long longFeature : longFeatures) {
			dataOutputStream.writeLong(longFeature);
		}

		dataOutputStream.writeInt(floatFeatureIndices.length);

		for (int floatFeatureIndex : floatFeatureIndices) {
			dataOutputStream.writeInt(floatFeatureIndex);
		}

		dataOutputStream.writeInt(floatFeatures.length);

		for (float floatFeature : floatFeatures) {
			dataOutputStream.writeFloat(floatFeature);
		}

		dataOutputStream.writeInt(binaryFeatureIndices.length);

		for (int binaryFeatureIndex : binaryFeatureIndices) {
			dataOutputStream.writeInt(binaryFeatureIndex);
		}

		dataOutputStream.writeInt(binaryFeatures.length);

		dataOutputStream.write(binaryFeatures);
	}

	@Override
	public void deserialize(DataInputStream dataInputStream) throws IOException {
		// Node id
		id = dataInputStream.readLong();

		// Node type
		type = dataInputStream.readInt();

		// Node weight
		weight = dataInputStream.readFloat();

		int edgeTypeNum = dataInputStream.readInt();

		edgeTypes = new int[edgeTypeNum];
		for (int i = 0; i < edgeTypeNum; i++) {
			edgeTypes[i] = i;
		}

		edgeAccSumWeights = new float[edgeTypeNum];
		for (int i = 0; i < edgeTypeNum; i++) {
			edgeAccSumWeights[i] = dataInputStream.readFloat();
		}

		edgeTotalSumWeights = dataInputStream.readFloat();

		neigborGroupIndices = new int[edgeTypeNum];
		for (int i = 0; i < edgeTypeNum; i++) {
			neigborGroupIndices[i] = dataInputStream.readInt();
		}

		int neighborsNum = dataInputStream.readInt();

		neighbors = new long[neighborsNum];
		for (int i = 0; i < neighborsNum; i++) {
			neighbors[i] = dataInputStream.readLong();
		}

		neighborAccSumWeights = new float[neighborsNum];
		for (int i = 0; i < neighborsNum; i++) {
			neighborAccSumWeights[i] = dataInputStream.readFloat();
		}

		int longFeatureNum = dataInputStream.readInt();

		longFeatureIndices = new int[longFeatureNum];
		for (int i = 0; i < longFeatureNum; i++) {
			longFeatureIndices[i] = dataInputStream.readInt();
		}

		int longFeatureValueNum = dataInputStream.readInt();

		longFeatures = new long[longFeatureValueNum];
		for (int i = 0; i < longFeatureValueNum; i++) {
			longFeatures[i] = dataInputStream.readLong();
		}

		int floatFeatureNum = dataInputStream.readInt();

		floatFeatureIndices = new int[floatFeatureNum];
		for (int i = 0; i < floatFeatureNum; i++) {
			floatFeatureIndices[i] = dataInputStream.readInt();
		}

		int floatFeatureValueNum = dataInputStream.readInt();

		floatFeatures = new float[floatFeatureValueNum];
		for (int i = 0; i < floatFeatureValueNum; i++) {
			floatFeatures[i] = dataInputStream.readFloat();
		}

		int binaryFeatureNum = dataInputStream.readInt();

		binaryFeatureIndices = new int[binaryFeatureNum];
		for (int i = 0; i < binaryFeatureNum; i++) {
			binaryFeatureIndices[i] = dataInputStream.readInt();
		}

		int binaryFeatureValueNum = dataInputStream.readInt();
		binaryFeatures = new byte[binaryFeatureValueNum];
		dataInputStream.readFully(binaryFeatures);
	}

	@Override
	public int dataLen() {
		return this.bufferLen();
	}
}
