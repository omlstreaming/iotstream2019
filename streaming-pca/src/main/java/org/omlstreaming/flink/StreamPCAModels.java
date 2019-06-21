package org.omlstreaming.flink;

import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.factory.DecompositionFactory_DDRM;
import org.ejml.interfaces.decomposition.QRDecomposition;
import org.ejml.simple.SimpleMatrix;

import java.io.Serializable;

class StreamPCAModelsState implements Serializable {

    private transient SimpleMatrix lambda;
    private transient SimpleMatrix Q;
    private transient SimpleMatrix xbar;
    private int n;
    private String pcaModelType;
    private int numPcaComps;
    private int winSize;

    void setLambda(SimpleMatrix lambda) {
        this.lambda = lambda;
    }

    void setQ(SimpleMatrix q) {
        Q = q;
    }

    void setN(int n) {
        this.n = n;
    }

    void setXbar(SimpleMatrix xbar) {
        this.xbar = xbar;
    }

    SimpleMatrix getLambda(){
        return lambda;
    }

    SimpleMatrix getQ(){
        return Q;
    }

    SimpleMatrix getXbar(){
        return xbar;
    }

    int getN(){
        return n;
    }

    String getPcaModelType() {
        return pcaModelType;
    }

    void setPcaModelType(String m) {
        pcaModelType = m;
    }

    void setNumPcaComps(int n) {
        numPcaComps = n;
    }

    int getNumPcaComps() {
        return numPcaComps;
    }

    void setWinSize(int s) {
        winSize = s;
    }

    public StreamPCAModelsState(int nC, int wS) {
        setNumPcaComps(nC);
        setWinSize(wS);
        setPcaModelType("ccpca");
        setLambda(new SimpleMatrix(getNumPcaComps(), 1));
        setQ(new SimpleMatrix(getNumPcaComps(), getNumPcaComps()));
        setXbar(new SimpleMatrix(getNumPcaComps(), 1));
        setN(0);
    }
}

public class StreamPCAModels implements Serializable{

    private transient StreamPCAModelsState modelState;

    public StreamPCAModelsState getModelState() {
        return modelState;
    }

    public void setModelState(StreamPCAModelsState modelState) {
        this.modelState = modelState;
    }

    public StreamPCAModels(String modelType, int nC, int wS, SimpleMatrix initEigVal, SimpleMatrix initEigVecs, SimpleMatrix pcaCenter, int iter){
        modelState = new StreamPCAModelsState(nC, wS);

        if (modelType != null) {
            modelState.setPcaModelType(modelType);
        }

        if (initEigVal != null) {
            modelState.setLambda(initEigVal);
        }

        if (initEigVecs != null) {
            modelState.setQ(initEigVecs);
        }

        if (pcaCenter != null) {
            modelState.setXbar(pcaCenter);
        }

        if (iter != 0) {
            modelState.setN(iter);
        }
    }


    // iterate a PCA model depending on the model
    StreamPCAModelsState iterateStreamPCAModel(StreamPCAModelsState state, SimpleMatrix x) {
        StreamPCAModelsState iteratedModel;
        switch (modelState.getPcaModelType()){
            case "ccpca" : // Covariance Free algorithm for PCA
                iteratedModel = CovarianceFreeIncrementalPCA(state, x);
                break;
            case "ghapca": // Generalized Hebbian Algorithm for PCA
                iteratedModel = GeneralizedHebbianPCA(state, x);
                break;
            case "sgaexpca": // Stochastic Gradient Ascent using exact QR decomposition
                iteratedModel = StochasticGradientAscentExactPCA(state, x);
                break;
            case "sgannpca": // Stochastic Gradient Ascent using a neural network
                iteratedModel = StochasticGradientAscentNeuralNetPCA(state, x);
                break;
            default:
                // Stochastic Gradient Ascent using exact QR decomposition
                iteratedModel = StochasticGradientAscentExactPCA(state, x);
                break;
        }
        return iteratedModel;
    }

    //     Covariance Free algorithm for PCA
    //     Weng et al. (2003). Candid Covariance-free Incremental Principal Component Analysis.
    //     IEEE Trans. Pattern Analysis and Machine Intelligence.

    private StreamPCAModelsState CovarianceFreeIncrementalPCA(StreamPCAModelsState state, SimpleMatrix x){

        //        The ’amnesic’ parameter l determines the weight of past observations in the PCA update. If l=0, all
        //        observations have equal weight, which is appropriate for stationary processes. Otherwise, typical
        //        values of l range between 2 and 4. As l increases, more weight is placed on new observations and
        //        less on older ones. For meaningful results, the condition 0<=l<n should hold.
        //        The algorithm iteratively updates the PCs while deflating x. If at some point the Euclidean
        //        norm of x becomes less than tol, the algorithm stops to prevent numerical overflow.

        // recover state
        SimpleMatrix lambda = state.getLambda();
        SimpleMatrix Q = state.getQ();
        int n = state.getN();
        SimpleMatrix xbar = state.getXbar();

        // update the average
        state.setXbar(this.updateIncrementalDataMean(xbar, x, n));
        xbar = state.getXbar();

        // for the update remove the average
        x = x.minus(xbar).transpose();

        // init
        int q = lambda.numRows();
        double l = 3;
        double tol = 1e-8;
        int i, d = x.getNumElements(), k = lambda.getNumElements();
        if (q != k) {
            Q.reshape(d,q);
            lambda.reshape(q, 1);
        }
        SimpleMatrix v;
        double f = (1.0 + l)/(1.0 + n);
        double nrm;

        for (i=0; i<q; i++) {

            nrm = x.normF();
            if (nrm < tol) {
                SimpleMatrix lambdaUpdate = lambda.extractMatrix(q - i, lambda.numRows(), 0, 1);
                lambda.setColumn(0, q - i, (lambdaUpdate.scale((1.0 - f))).getDDRM().data);
                break;
            }

            if (i == n) {
                lambda.set(i, 0, nrm);
                double[] xNormedValues = (x.scale(1.0 / nrm)).getDDRM().data;
                Q.setColumn(i, 0, xNormedValues);
                break;
            }

            v = Q.extractVector(false, i).scale(((1.0 - f) * lambda.get(i,0)))
                    .plus(x.scale(f * (Q.extractVector(false, i).dot(x))));

            nrm = v.normF();
            if (nrm < tol) {
                lambda.set(i, 0, 0.0);
                break;
            }
            lambda.set(i, 0, nrm);
            double[] vNormedValues = (v.scale(1.0 / nrm)).getDDRM().data;
            Q.setColumn(i, 0, vNormedValues);
            x = x.minus(Q.extractVector(false, i)
                    .scale(Q.extractVector(false, i)
                            .dot(x)));
        }
        // populate new state
        state.setLambda(lambda);
        state.setQ(Q);
        return state;
    }

    //     Generalized Hebbian Algorithm for PCA
    //     Sanger (1989). Optimal unsupervised learning in a single-layer linear feedforward neural network.
    //     Neural Networks Journal

    private StreamPCAModelsState GeneralizedHebbianPCA(StreamPCAModelsState state, SimpleMatrix x){

        //        The vector gamma determines the weight placed on the new data in updating each eigenvector (the
        //        first coefficient of gamma corresponds to the first eigenvector, etc). It can be specified as a single
        //        positive number or as a vector of length ncol(U). Larger values of gamma place more weight on
        //        x and less on U. A common choice for (the components of) gamma is of the form c/n, with n the
        //        sample size and c a suitable positive constant.

        // recover state
        SimpleMatrix lambda = state.getLambda();

        // force copy of all internal data from state!
        SimpleMatrix Q = state.getQ().createLike();
        Q.getDDRM().data = state.getQ().getDDRM().data;

        int ind = state.getN();
        SimpleMatrix gamma = new SimpleMatrix(Q.numCols(), 1);
        for (int id = 0; id < Q.numCols(); id++) {
            gamma.set(id, 1.0);
        }
        gamma = gamma.scale(1.0 / (ind * ind));
        SimpleMatrix xbar = state.getXbar();

        // update the average
        state.setXbar(this.updateIncrementalDataMean(xbar, x, ind));
        xbar = state.getXbar();

        // for the update remove the average
        x = x.minus(xbar).transpose();

        // update the predictor
        SimpleMatrix y = Q.mult(x);

        // prepare new state
        int m = Q.numRows(), n = Q.numCols();
        SimpleMatrix gamy = gamma.elementMult(y); // Schur product
        SimpleMatrix b = Q.extractVector(false, 0).scale(y.get(0));
        SimpleMatrix A = new SimpleMatrix(m,n);
        A.setColumn(0, 0,
                (Q.extractVector(false, 0)
                        .minus(b.scale(gamy.get(0))))
                        .getDDRM()
                        .data);
        for (int i=1; i<n; i++) {
            b = b.plus(Q.extractVector(false, i).scale(y.get(i)));
            A.setColumn(i, 0,
                    (Q.extractVector(false, i)
                            .minus(b.scale(gamy.get(i))))
                            .getDDRM()
                            .data);
        }
        A = A.plus(x.mult(gamy.transpose()));
        SimpleMatrix decay = ((gamma.minus(1.0)).scale(-1.0)).elementMult(lambda);
        SimpleMatrix increment = gamma.elementMult(y).elementMult(y);
        lambda = increment.plus(decay);

        // wrap return values
        state.setLambda(lambda);
        state.setQ(A);
        return state;
    }

    //     Stochastic Gradient Ascent PCA - Exact, QR decomposition based version
    //     Oja (1992). Principal components, Minor components, and linear neural networks. Neural Networks.

    private StreamPCAModelsState StochasticGradientAscentExactPCA(StreamPCAModelsState state, SimpleMatrix x){

        //        The gain vector gamma determines the weight placed on the new data in updating each principal
        //        component. The first coefficient of gamma corresponds to the first principal component, etc.. It can
        //        be specified as a single positive number (which is recycled by the function) or as a vector of length
        //        ncol(U). For larger values of gamma, more weight is placed on x and less on U. A common choice
        //        for (the components of) gamma is of the form c/n, with n the sample size and c a suitable positive
        //        constant. The Stochastic Gradient Ascent PCA can be implemented exactly or through a neural network.
        //        The latter is less accurate but faster.

        // recover state
        SimpleMatrix lambda = state.getLambda();

        // force copy of all internal data from state!
        SimpleMatrix Q = state.getQ().createLike();
        Q.getDDRM().data = state.getQ().getDDRM().data;

        int ind = state.getN();
        SimpleMatrix gamma = new SimpleMatrix(Q.numCols(), 1);
        for (int id = 0; id < Q.numCols(); id++) {
            gamma.set(id, 1.0);
        }
        gamma = gamma.scale(1.0 / (ind * ind));
        SimpleMatrix xbar = state.getXbar();

        // update the average
        // update the average
        state.setXbar(this.updateIncrementalDataMean(xbar, x, ind));
        xbar = state.getXbar();

        // for the update remove the average
        x = x.minus(xbar).transpose();

        // update the predictor
        SimpleMatrix y = Q.mult(x);

        SimpleMatrix W;
        SimpleMatrix Qupd = Q;
        SimpleMatrix evidence = (x.mult(y.transpose()));
        SimpleMatrix gammaDiag = gamma.diag();
        SimpleMatrix increment = evidence.mult(gammaDiag);
        Qupd = Qupd.plus(increment);

        QRDecomposition<DMatrixRMaj> qrDecomp = DecompositionFactory_DDRM.qr(Qupd.numRows(), Qupd.numCols());
        qrDecomp.decompose(Qupd.getMatrix());
        W = SimpleMatrix.wrap(qrDecomp.getQ(null, true));

        SimpleMatrix decay = ((gamma.minus(1.0)).scale(-1.0)).elementMult(lambda);
        SimpleMatrix incrementLambda = gamma.elementMult(y).elementMult(y);
        lambda = incrementLambda.plus(decay);

        // wrap return values
        state.setLambda(lambda);
        state.setQ(W);
        return state;
    }


    // Stochastic Gradient Ascent PCA - Fast Neural Network version
    // Oja (1992). Principal components, Minor components, and linear neural networks. Neural Networks.

    private StreamPCAModelsState StochasticGradientAscentNeuralNetPCA(StreamPCAModelsState state, SimpleMatrix x){

        // recover state
        SimpleMatrix lambda = state.getLambda();

        // force copy of all internal data from state!
        SimpleMatrix Q = state.getQ().createLike();
        Q.getDDRM().data = state.getQ().getDDRM().data;

        int ind = state.getN();
        SimpleMatrix gamma = new SimpleMatrix(Q.numCols(), 1);
        for (int id = 0; id < Q.numCols(); id++) {
            gamma.set(id, 1.0);
        }
        gamma = gamma.scale(1.0 / (ind * ind));
        SimpleMatrix xbar = state.getXbar();

        // update the average
        state.setXbar(this.updateIncrementalDataMean(xbar, x, ind));
        xbar = state.getXbar();

        // for the update remove the average
        x = x.minus(xbar).transpose();

        // update the predictor
        SimpleMatrix y = Q.mult(x);

        int m = Q.numRows(), n = Q.numCols();
        SimpleMatrix gamy = gamma.elementMult(y); // Schur product
        SimpleMatrix b = Q.extractVector(false, 0).scale(y.get(0));
        SimpleMatrix A = new SimpleMatrix(m,n);
        A.setColumn(0, 0,
                (Q.extractVector(false, 0)
                        .minus(b.scale(gamy.get(0))))
                        .getDDRM()
                        .data);
        for (int i=1; i<n; i++) {

            b = b.plus((Q.extractVector(false, i - 1).scale(y.get(i - 1)))
                    .plus(Q.extractVector(false, i).scale(y.get(i))));

            A.setColumn(i, 0,
                    (Q.extractVector(false, i)
                            .minus(b.scale(gamy.get(i))))
                            .getDDRM()
                            .data);
        }
        A = A.plus(x.mult(gamy.transpose()));
        SimpleMatrix decay = ((gamma.minus(1.0)).scale(-1.0)).elementMult(lambda);
        SimpleMatrix increment = gamma.elementMult(y).elementMult(y);
        lambda = increment.plus(decay);

        // wrap return values
        state.setLambda(lambda);
        state.setQ(A);
        return state;
    }

    // Recursive update of the sample mean vector used in all PCA algorithms.
    private SimpleMatrix updateIncrementalDataMean(SimpleMatrix xbar, SimpleMatrix x, int n) {
        //    The forgetting factor f determines the balance between past and present observations in the PCA
        //    update: the closer it is to 1 (resp. to 0), the more weight is placed on current and past observations.
        //    For a given argument n, the default value of f is 1/(n + 1).
        double f = 1.0 / (n + 1.0);
        return (xbar.scale(1.0 - f)).plus(x.scale(f));
    }
}
