package org.omlstreaming.flink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.ejml.simple.SimpleMatrix;

import java.util.List;


public abstract class PCAGlobalFeatureEigVecs extends MultiDimWindowGlobalFeature {

    protected String typeClass = "";

    final int pcaEigVecsPos = 0;
    final int stateArity = 1;

    int nC;
    int wS;
    String modelType;
    SimpleMatrix initEigVal;
    SimpleMatrix initEigVecs;
    SimpleMatrix pcaCenter;
    int iter;

    private StreamPCAModels pcaModel;

    PCAGlobalFeatureEigVecs(int featureField, String modelTypeInit,
                            int nCInit, int wSInit, SimpleMatrix initEigValInit,
                            SimpleMatrix initEigVecsInit, SimpleMatrix pcaCenterInit, int iterInit) {
        super(featureField);
        nC = nCInit;
        wS = wSInit;
        initEigVal = initEigValInit;
        initEigVecs = initEigVecsInit;
        pcaCenter = pcaCenterInit;
        iter = iterInit;
        modelType = modelTypeInit;
        pcaModel = new StreamPCAModels(modelType, nC, wS, initEigVal, initEigVecs, pcaCenter, iter);
    }

    @Override
    public CustomStateGlobalFeature newInstance() {
        return PCAGlobalFeatureEigVecs.create(typeClass, featureField, modelType, nC, wS, initEigVal, initEigVecs, pcaCenter, iter);
    }

    public StreamPCAModels getPcaModel() {
        return pcaModel;
    }

    public void setPcaModel(StreamPCAModels m) {
        pcaModel = m;
    }

    public static PCAGlobalFeatureEigVecs create(String type, int featureField, String modelType,
                                                 int nC, int wS, SimpleMatrix initEigVal,
                                                 SimpleMatrix initEigVecs, SimpleMatrix pcaCenter, int iter) {
        PCAGlobalFeatureEigVecs result;
        switch (type.toLowerCase()) {
            case "byte":
            case "short":
            case "int":
            case "integer":
            case "long":
            case "bigint":
            case "float":
            case "double":
            case "bigdecimal":
            case "decimal":
                        result = new DoublePCAGlobalFeatureEigVecs(featureField, modelType, nC, wS, initEigVal, initEigVecs, pcaCenter, iter);
                break;
            default:
                result = null;
                break;
        }
        return result;
    }

    @Override
    public int compare(Object globalFeature, Object value) {
        throw new UnsupportedOperationException("Streaming PCA Eigvals are not comparable");
    }

    public Object accumulate(Object globalFeature, Number element) {
        throw new UnsupportedOperationException("Streaming PCA Eigvals accumulate on single element not supported.");
    }

    @Override
    public Object accumulate(Object globalFeature, Row element) {
        throw new UnsupportedOperationException("Streaming PCA Eigvals accumulate on row not supported.");
    }

    @Override
    public Object update(Object globalFeature, Row element, List<Row> updated) {
        throw new UnsupportedOperationException("Streaming PCA Eigvals update not supported.");
    }

    @Override
    public Object retract(Object globalFeature, List<Row> updated) {
        throw new UnsupportedOperationException("Streaming PCA Eigvals retract not supported.");
    }

    public static class DoublePCAGlobalFeatureEigVecs extends PCAGlobalFeatureEigVecs {

        private static final long serialVersionUID = 1L;

        DoublePCAGlobalFeatureEigVecs(int featureField, String modelType, int nC, int wS, SimpleMatrix initEigVal, SimpleMatrix initEigVecs, SimpleMatrix pcaCenter, int iter) {
            super(featureField, modelType, nC, wS, initEigVal, initEigVecs, pcaCenter, iter);
            setPcaModel(new StreamPCAModels(modelType, nC, wS, initEigVal, initEigVecs, pcaCenter, iter));
            this.typeClass = "double";
        }

        @Override
        public Object accumulate(Object globalFeature, Row arg0) {
            Number[][] arg0Num = new Number[wS][arg0.getArity() - 1];
            for (int wId = 0; wId < wS; wId++) {
                for (int id = 1; id < arg0.getArity(); id++) {
                    arg0Num[wId][id - 1] = (Number) arg0.getField(id);
                }
            }
            return this.accumulate(globalFeature, arg0Num);
        }

        @Override
        public Object accumulate(Object globalFeature, Number[][] elements) {
            double[][] values = new double[wS][elements[0].length];
            for (int wId = 0; wId < wS; wId++) {
                for (int sId = 0; sId < elements[0].length; sId++) {
                    values[wId][sId] = (double) elements[wId][sId];
                }
            }
            return this.accumulate(globalFeature, values);
        }

        public Object accumulate(Object globalFeature, double[][] values) {

            Row state = (Row) globalFeature;
            SimpleMatrix Q = (SimpleMatrix) state.getField(pcaEigVecsPos);

            this.getPcaModel().getModelState().setQ(Q);

            SimpleMatrix dataIn = new SimpleMatrix(values);
            this.getPcaModel().setModelState(this.getPcaModel().iterateStreamPCAModel(this.getPcaModel().getModelState(), dataIn));

            Q = this.getPcaModel().getModelState().getQ();

            state.setField(pcaEigVecsPos, Q);

            return state;

        }

        @Override
        public Object getFeature(Object globalFeature) {

            SimpleMatrix Q = (SimpleMatrix) ((Row) globalFeature).getField(pcaEigVecsPos);
            double[][] eigVecs = new double[Q.numRows()][Q.numCols()];
            for (int id = 0; id < Q.numRows(); id++){
                for (int jd = 0; jd < Q.numCols(); jd++){
                    eigVecs[id][jd] = Q.get(id, jd);
                }
            }
            return eigVecs;
        }

        @Override
        public Row createCustomStateObject(Object state) {

            Row rState = (Row) state;
            Row flinkState = new Row(stateArity);

            SimpleMatrix Q = (SimpleMatrix) rState.getField(pcaEigVecsPos);
            double[] qData = Q.getDDRM().data;
            int qR = Q.numRows();
            int qC = Q.numCols();
            flinkState.setField(pcaEigVecsPos, qData);
            flinkState.setField(pcaEigVecsPos + 1, qR);
            flinkState.setField(pcaEigVecsPos + 2, qC);

            return flinkState;
        }


        @Override
        public Object initializeCustomStateObject(Row initState) {

            Row state = new Row(stateArity);

            double[] qData = (double[]) initState.getField(pcaEigVecsPos);
            int qR = (int) initState.getField(pcaEigVecsPos + 1);
            int qC = (int) initState.getField(pcaEigVecsPos + 2);
            SimpleMatrix Q = new SimpleMatrix(qR, qC, true, qData);

            state.setField(pcaEigVecsPos, Q);

            return state;
        }


        @Override
        public Object init() {

            Row state = new Row(stateArity);
            this.setPcaModel((new StreamPCAModels(modelType, nC, wS, initEigVal, initEigVecs, pcaCenter, iter)));
            state.setField(pcaEigVecsPos, this.getPcaModel().getModelState().getQ());

            return state;
        }

        @Override
        public TypeInformation<?> getFeatureType() {

            String[] fieldNames = { "Q", "qR", "qC"};

            TypeInformation<?>[] types = new TypeInformation<?>[fieldNames.length];

            types[pcaEigVecsPos] = PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
            types[pcaEigVecsPos + 1] = BasicTypeInfo.INT_TYPE_INFO; // no. rows
            types[pcaEigVecsPos + 2] = BasicTypeInfo.INT_TYPE_INFO; // no. cols

            RowTypeInfo rowType = new RowTypeInfo(types, fieldNames);

            return rowType;
        }

        @Override
        public TypeInformation<?> getFeatureResultType() {
            return PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
        }


    }
}
