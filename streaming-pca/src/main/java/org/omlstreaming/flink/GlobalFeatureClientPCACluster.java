package org.omlstreaming.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.ejml.data.DMatrixRMaj;
import org.ejml.ops.MatrixIO;
import org.ejml.simple.SimpleMatrix;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class GFETestingProperties {
    ArrayList<String> result;
    InputStream inputStream;

    public GFETestingProperties(){
        result = new ArrayList<>();
        inputStream = null;
    }

    public ArrayList<String> getPropValues() {

        try {
            Properties prop = new Properties();

            prop.load(GFETestingProperties.class.getResourceAsStream("/perf_test_config.properties"));

            // get the property value and print it out
            String gfeFeatures = prop.getProperty("features");
            String gfeBackend = prop.getProperty("backend");
            String gfeInfra = prop.getProperty("infrastructure");
            String gfePort = prop.getProperty("cport");
            String gfeIP = prop.getProperty("cip");

            result.add(gfeFeatures);
            result.add(gfeBackend);
            result.add(gfeInfra);
            result.add(gfePort);
            result.add(gfeIP);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}

public class GlobalFeatureClientPCACluster {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure(new NullAppender());

        GFETestingProperties testConf = new GFETestingProperties();
        ArrayList<String> paramTest = testConf.getPropValues();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String[] featuresList = paramTest.get(0).split(",");
        String backendType = paramTest.get(1);
        String infraType = paramTest.get(2);
        int commPort = Integer.valueOf(args[0]);
        String commIP = paramTest.get(4);

        long winSize = 1000;

        if (backendType.equals("Local")) {
            final String filePath = "./dataDisk";
            int iFile = 1;
            while (Files.exists(Paths.get(filePath + iFile))) {
                Files.deleteIfExists(Paths.get(filePath + iFile));
                iFile++;
            }
        }

        List<GlobalFeature> testFeaturesList = new ArrayList<>();

        String[] featuresTypeNames = {"Long", "Double", "Double", "Double", "Double", "Double", "Double", "Double", "Double", "Double", "Double"};
        String[] featuresNames = {"ts", "val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8", "val9", "val0"};
        RowTypeInfo inputInputType = FeatureUtil
                .createFeatureRowTypeInfo(featuresNames, featuresTypeNames);

        String[] gfTypeNames = {"Long", "Double", "Double", "Double", "Double", "Double", "Double", "Double", "Double", "Double", "Double"};
        String[] gfNamesTyped = {"id", "pc1", "pc2", "pc3", "pc4", "pc5", "pc6", "pc7", "pc8", "pc9", "pc10"};
        RowTypeInfo gfInputType = FeatureUtil
                .createFeatureRowTypeInfo(gfNamesTyped, gfTypeNames);

        StorageBackendFixSize backend = StorageBackendFixSize.createBackend(
                backendType,
                gfInputType.createSerializer(null),
                FeatureUtil.getRowFixByteSize(gfInputType),
                gfInputType);

        int id = (new Random()).nextInt(24);

        boolean dumpFeaturesAndProfiling = false;

        DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        Date date = new Date();
        String valsEvalFile = null;
        String perfEvalFile = null;
        String pcaMethod = "sgannpca"; // ccpca, ghapca, sgaexpca, sgannpca

        if (dumpFeaturesAndProfiling){
             valsEvalFile = "gfe_output_" + pcaMethod + commPort + "_" + id + "_" + dateFormat.format(date) + "/" +
                    infraType + "/" +
                    backendType + "/" +
                    dateFormat.format(date) + "_feature_vals_" +
                    Stream.of(featuresList).collect(Collectors.joining()) + "_" +
                    gfNamesTyped[1] + "_" +
                    gfTypeNames[1] + ".csv";
             perfEvalFile = "gfe_output_" + pcaMethod + commPort + "_" + id + "_" + dateFormat.format(date) + "/" +
                    infraType + "/" +
                    backendType + "/" +
                    dateFormat.format(date) + "_profiling_" +
                    Stream.of(featuresList).collect(Collectors.joining()) + "_" +
                    gfNamesTyped[1] + "_" +
                    gfTypeNames[1] + ".csv";
        }

        String resEvalFile =  "gfe_output_" + pcaMethod +  commPort + "_" + id + "_"  + dateFormat.format(date) + "/" +
                infraType + "/" +
                backendType + "/" +
                dateFormat.format(date) + "_latency_" +
                Stream.of(featuresList).collect(Collectors.joining()) + "_" +
                gfNamesTyped[1] + "_" +
                gfTypeNames[1];
        String thrEvalFile =  "gfe_output_" + pcaMethod + commPort + "_" + id + "_"  + dateFormat.format(date) + "/" +
                infraType + "/" +
                backendType + "/" +
                dateFormat.format(date) + "_throughput_" +
                Stream.of(featuresList).collect(Collectors.joining()) + "_" +
                gfNamesTyped[1] + "_" +
                gfTypeNames[1];

        String initEigValsFile = args[1];
        String initEigVectFile = args[2];
        String initPCACenterFile = args[3];
        Long operatorWindowSize = Long.parseLong(args[4]);

        int initialSampleSize = 5000;

        try {
            DMatrixRMaj initEigValRead = MatrixIO
                    .loadCSV(initEigValsFile
                            , true);
            SimpleMatrix initEigVals = SimpleMatrix.wrap(initEigValRead);
            DMatrixRMaj initEigVectRead = MatrixIO
                    .loadCSV(initEigVectFile, true);
            SimpleMatrix initEigVecs = SimpleMatrix.wrap(initEigVectRead);
            DMatrixRMaj initPCACenterRead = MatrixIO
                    .loadCSV(initPCACenterFile, true);
            SimpleMatrix initPCACenter = SimpleMatrix.wrap(initPCACenterRead).transpose();

            testFeaturesList.add((GlobalFeature) IdentityGlobalFeature.create("Long", 0));

            if (Arrays.asList(featuresList).contains("PCAvecs")) {
                PCAGlobalFeatureEigVecs featPCAvecs = PCAGlobalFeatureEigVecs
                        .create("Double",
                                0,
                                pcaMethod,
                                initEigVecs.numRows(),
                                1,
                                initEigVals,
                                initEigVecs,
                                initPCACenter,
                                initialSampleSize);
                testFeaturesList.add((GlobalFeature) featPCAvecs);
            }

            if (Arrays.asList(featuresList).contains("PCAvals")) {
                PCAGlobalFeatureEigVals featPCAvals = PCAGlobalFeatureEigVals
                        .create("Double",
                                0,
                                pcaMethod,
                                initEigVecs.numRows(),
                                1,
                                initEigVals,
                                initEigVecs,
                                initPCACenter,
                                initialSampleSize);
                testFeaturesList.add((GlobalFeature) featPCAvals);
            }

            DataStream<Row> outputF = env.socketTextStream(commIP, commPort)
                    .map(new MapFunction<String, Row>() {
                        @Override
                        public Row map(String arg0) throws Exception {
                            String[] parsed = arg0.split(",");
                            double[] inVec = new double[parsed.length - 1];
                            Row ret = new Row(parsed.length);
                            ret.setField(0, Long.parseLong(parsed[0]));
                            for (int id = 1; id < parsed.length; id++) {
                                inVec[id - 1] = Double.parseDouble(parsed[id]);
                            }
                            for (int id = 1; id < parsed.length; id++) {
                                ret.setField(id, (Double) inVec[id - 1]);
                            }
                            return ret;
                        }
                    })
                    .keyBy(new KeySelector<Row, Object>() {
                        @Override
                        public Object getKey(Row row) throws Exception {
                            return 0L;
                        }
                    })
                    .process(new ContinuousFeatureProcessFunctionLatency(
                            TimeUnit.HOURS.toMillis(operatorWindowSize),
                            0,
                            testFeaturesList,
                            backend,
                            inputInputType,
                            gfInputType,
                            backend.getAccessParametersType(),
                            perfEvalFile,
                            valsEvalFile,
                            featuresList
                    ))
                    .setParallelism(1);
            outputF.writeAsText(resEvalFile, FileSystem.WriteMode.NO_OVERWRITE);
            outputF.timeWindowAll(Time.milliseconds(winSize))
                    .apply(new AllWindowFunction<Row, Row, TimeWindow>() {
                        @Override
                        public void apply(TimeWindow timeWindow,
                                          Iterable<Row> iterable,
                                          Collector<Row> collector) throws Exception {
                            Iterator<Row> iterFld = iterable.iterator();
                            long count = 0;
                            while (iterFld.hasNext()) {
                                iterFld.next();
                                count++;
                            }
                            Row result = new Row(1);
                            result.setField(0, count);
                            collector.collect(result);
                        }
                    })
                    .setParallelism(1)
                    .writeAsText(thrEvalFile, FileSystem.WriteMode.NO_OVERWRITE);
            env.execute("Streaming PCA");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}