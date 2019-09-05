package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.graph.data.feature.FloatFeatures;
import com.tencent.angel.graph.data.graph.Node;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetNodeFloatFeature extends GetFunc {
	/**
	 * Create a new GetNodeFloatFeature.
	 *
	 * @param param parameter
	 */
	public GetNodeFloatFeature(GetNodeFeatureParam param) {
		super(param);
	}

	/**
	 * Create a empty GetNodeFloatFeature
	 */
	public GetNodeFloatFeature() {
		this(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartGetNodeFeatureParam param = (PartGetNodeFeatureParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		// Results
		FloatFeatures[] results = new FloatFeatures[param.getNodeIds().length];

		// Get feature for each node
		long[] nodeIds = param.getNodeIds();
		for (int i = 0; i < nodeIds.length; i++) {
			results[i] = getFeature(row.getNode(nodeIds[i]), param.getFids());
		}

		return new PartGetNodeFloatFeatureResult(part.getPartitionKey().getPartitionId(), results);
	}

	private FloatFeatures getFeature(Node node, int[] fids) {
		int[] featureSizes = new int[fids.length];
		List<Float> featureValues = new ArrayList<>();

		int[] floatFeatureIndices = node.getFloatFeatureIndices();
		float[] floatFeatures = node.getFloatFeatures();

		// Get feature through feature index
		for (int i = 0; i < fids.length; i++) {
			int fid = fids[i];
			if (fid >= 0 && fid < floatFeatureIndices.length) {
				int pre = fid == 0 ? 0 : floatFeatureIndices[fid - 1];
				int cur = floatFeatureIndices[fid];
				for (int fidx = pre; fidx < cur; fidx++) {
					featureValues.add(floatFeatures[fidx]);
				}
				featureSizes[i] = cur - pre;
			} else {
				featureSizes[i] = 0;
			}
		}
		return new FloatFeatures(featureSizes, featureValues.toArray(new Float[0]));
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		int[] offsets = ((GetNodeFeatureParam) param).getOffsets();
		long[] nodeIds = ((GetNodeFeatureParam) param).getNodeIds();
		int len = ((GetNodeFeatureParam) param).getNodeIds().length;

		Map<Long, FloatFeatures> features = new Long2ObjectOpenHashMap<>(len);
		for (PartitionGetResult result : partResults) {
			PartGetNodeFloatFeatureResult getNodeFeatureResult = (PartGetNodeFloatFeatureResult) result;
			FloatFeatures[] nodeResults = getNodeFeatureResult.getNodeFeatures();
			int startIndex = getNodeFeatureResult.getPartId() == 0 ? 0 : offsets[getNodeFeatureResult.getPartId() - 1];
			int endIndex = offsets[getNodeFeatureResult.getPartId()];
			for (int i = startIndex; i < endIndex; i++) {
				features.put(nodeIds[i], nodeResults[i - startIndex]);
			}
		}

		return new GetNodeFloatFeatureResult(features);
	}
}
