package org.omlstreaming.flink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.ejml.simple.SimpleMatrix;

import java.util.List;


public abstract class PCAGlobalFeatureEigVals extends MultiDimWindowGlobalFeature {

    protected String typeClass = "";

    final int pcaEigValsPos = 0;
    final int stateArity = 1;

    int nC;
    int wS;
    String modelType;
    SimpleMatrix initEigVal;
    SimpleMatrix initEigVecs;
    SimpleMatrix pcaCenter;
    int iter;

    private StreamPCAModels pcaModel;

    PCAGlobalFeatureEigVals(int featureField, String modelTypeInit,
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
        return PCAGlobalFeatureEigVals.create(typeClass, featureField, modelType, nC, wS, initEigVal, initEigVecs, pcaCenter, iter);
    }

    public StreamPCAModels getPcaModel() {
        return pcaModel;
    }

    public void setPcaModel(StreamPCAModels m) {
        pcaModel = m;
    }

    public static PCAGlobalFeatureEigVals create(String type, int featureField, String modelType,
                                                 int nC, int wS, SimpleMatrix initEigVal,
                                                 SimpleMatrix initEigVecs, SimpleMatrix pcaCenter, int iter) {
        PCAGlobalFeatureEigVals result;
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
                        result = new DoublePCAGlobalFeatureEigVals(featureField, modelType, nC, wS, initEigVal, initEigVecs, pcaCenter, iter);
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

    public static class DoublePCAGlobalFeatureEigVals extends PCAGlobalFeatureEigVals {

        private static final long serialVersionUID = 1L;

        DoublePCAGlobalFeatureEigVals(int featureField, String modelType, int nC, int wS, SimpleMatrix initEigVal, SimpleMatrix initEigVecs, SimpleMatrix pcaCenter, int iter) {
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
            SimpleMatrix lambda = (SimpleMatrix) state.getField(pcaEigValsPos);

            this.getPcaModel().getModelState().setLambda(lambda);

            SimpleMatrix dataIn = new SimpleMatrix(values);
            this.getPcaModel().setModelState(this.getPcaModel().iterateStreamPCAModel(this.getPcaModel().getModelState(), dataIn));

            lambda = this.getPcaModel().getModelState().getLambda();

            state.setField(pcaEigValsPos, lambda);

            return state;

        }

        @Override
        public Object getFeature(Object globalFeature) {

            SimpleMatrix lambda = (SimpleMatrix) ((Row) globalFeature).getField(pcaEigValsPos);
            double[][] eigVals = new double[lambda.numRows()][lambda.numCols()];
            for (int id = 0; id < lambda.numRows(); id++){
                for (int jd = 0; jd < lambda.numCols(); jd++){
                    eigVals[id][jd] = lambda.get(id, jd);
                }
            }
            return eigVals;
        }

        @Override
        public Row createCustomStateObject(Object state) {

            Row rState = (Row) state;
            Row flinkState = new Row(stateArity);

            // Lambda to flink state
            SimpleMatrix lambda = (SimpleMatrix) rState.getField(pcaEigValsPos);
            double[] lambdaData = lambda.getDDRM().data;
            int lambdaR = lambda.numRows();
            int lambdaC = lambda.numCols();
            flinkState.setField(pcaEigValsPos, lambdaData);
            flinkState.setField(pcaEigValsPos + 1, lambdaR);
            flinkState.setField(pcaEigValsPos + 2, lambdaC);

            return flinkState;
        }


        @Override
        public Object initializeCustomStateObject(Row initState) {

            Row state = new Row(stateArity);

            // lambda revert
            double[] lambdaData = (double[]) initState.getField(pcaEigValsPos);
            int lambdaR = (int) initState.getField(pcaEigValsPos + 1);
            int lambdaC = (int) initState.getField(pcaEigValsPos + 2);
            SimpleMatrix lambda = new SimpleMatrix(lambdaR, lambdaC, true, lambdaData);

            state.setField(pcaEigValsPos, lambda);

            return state;
        }


        @Override
        public Object init() {

            Row state = new Row(stateArity);
            setPcaModel((new StreamPCAModels(modelType, nC, wS, initEigVal, initEigVecs, pcaCenter, iter)));
            state.setField(pcaEigValsPos, getPcaModel().getModelState().getLambda());

            return state;
        }

        @Override
        public TypeInformation<?> getFeatureType() {

            String[] fieldNames = { "lambda", "lambdaR", "lambdaC"};

            TypeInformation<?>[] types = new TypeInformation<?>[fieldNames.length];

            types[pcaEigValsPos] = PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
            types[pcaEigValsPos + 1] = BasicTypeInfo.INT_TYPE_INFO;
            types[pcaEigValsPos + 2] = BasicTypeInfo.INT_TYPE_INFO;

            RowTypeInfo rowType = new RowTypeInfo(types, fieldNames);

            return rowType;
        }

        @Override
        public TypeInformation<?> getFeatureResultType() {
            return PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO;
        }


    }
}
