package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.graph.data.feature.BinaryFeatures;
import com.tencent.angel.graph.data.graph.Node;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetNodeBinaryFeature extends GetFunc {
	/**
	 * Create a new GetNodeBinaryFeature.
	 *
	 * @param param parameter
	 */
	public GetNodeBinaryFeature(GetNodeFeatureParam param) {
		super(param);
	}

	/**
	 * Create a empty GetNodeBinaryFeature
	 */
	public GetNodeBinaryFeature() {
		this(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartGetNodeFeatureParam param = (PartGetNodeFeatureParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		// Results
		BinaryFeatures[] results = new BinaryFeatures[param.getNodeIds().length];

		// Get feature for each node
		long[] nodeIds = param.getNodeIds();
		for (int i = 0; i < nodeIds.length; i++) {
			results[i] = getFeature(row.getNode(nodeIds[i]), param.getFids());
		}

		return new PartGetNodeBinaryFeatureResult(part.getPartitionKey().getPartitionId(), results);
	}

	private BinaryFeatures getFeature(Node node, int[] fids) {
		int[] featureSizes = new int[fids.length];
		List<Byte> featureValues = new ArrayList<>();

		int[] binaryFeatureIndices = node.getBinaryFeatureIndices();
		byte[] binaryFeatures = node.getBinaryFeatures();

		// Get feature through feature index
		for (int i = 0; i < fids.length; i++) {
			int fid = fids[i];
			if (fid >= 0 && fid < binaryFeatureIndices.length) {
				int pre = fid == 0 ? 0 : binaryFeatureIndices[fid - 1];
				int cur = binaryFeatureIndices[fid];
				for (int fidx = pre; fidx < cur; fidx++) {
					featureValues.add(binaryFeatures[fidx]);
				}
				featureSizes[i] = cur - pre;
			} else {
				featureSizes[i] = 0;
			}
		}
		return new BinaryFeatures(featureSizes, featureValues.toArray(new Byte[0]));
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		int[] offsets = ((GetNodeFeatureParam) param).getOffsets();
		long[] nodeIds = ((GetNodeFeatureParam) param).getNodeIds();
		int len = ((GetNodeFeatureParam) param).getNodeIds().length;

		Map<Long, BinaryFeatures> features = new Long2ObjectOpenHashMap<>(len);
		for (PartitionGetResult result : partResults) {
			PartGetNodeBinaryFeatureResult getNodeFeatureResult = (PartGetNodeBinaryFeatureResult) result;
			BinaryFeatures[] nodeResults = getNodeFeatureResult.getNodeFeatures();
			int startIndex = getNodeFeatureResult.getPartId() == 0 ? 0 : offsets[getNodeFeatureResult.getPartId() - 1];
			int endIndex = offsets[getNodeFeatureResult.getPartId()];
			for (int i = startIndex; i < endIndex; i++) {
				features.put(nodeIds[i], nodeResults[i - startIndex]);
			}
		}

		return new GetNodeBinaryFeatureResult(features);
	}
}
