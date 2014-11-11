package edu.unc.mapseq.commands.nec.variantcalling;

import java.util.concurrent.Executors;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.commands.Option;
import org.apache.karaf.shell.console.AbstractAction;

import edu.unc.mapseq.commons.nec.variantcalling.SaveDepthOfCoverageAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBean;

@Command(scope = "nec-variant-calling", name = "save-depth-of-coverage-attributes", description = "Save DepthOfCoverage Attributes")
public class SaveDepthOfCoverageAttributesAction extends AbstractAction {

    @Option(name = "--sampleId", description = "sampleId", required = false, multiValued = false)
    private Long sampleId;

    @Option(name = "--flowcellId", description = "flowcellId", required = false, multiValued = false)
    private Long flowcellId;

    private MaPSeqDAOBean maPSeqDAOBean;

    public SaveDepthOfCoverageAttributesAction() {
        super();
    }

    @Override
    public Object doExecute() {

        if (flowcellId == null && sampleId == null) {
            System.out.println("Both sampleId && flowcellId can't be null");
            return null;
        }

        SaveDepthOfCoverageAttributesRunnable runnable = new SaveDepthOfCoverageAttributesRunnable();
        runnable.setMapseqDAOBean(maPSeqDAOBean);
        if (sampleId != null) {
            runnable.setSampleId(sampleId);
        }
        if (flowcellId != null) {
            runnable.setFlowcellId(flowcellId);
        }
        Executors.newSingleThreadExecutor().execute(runnable);

        return null;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public MaPSeqDAOBean getMaPSeqDAOBean() {
        return maPSeqDAOBean;
    }

    public void setMaPSeqDAOBean(MaPSeqDAOBean maPSeqDAOBean) {
        this.maPSeqDAOBean = maPSeqDAOBean;
    }

}
