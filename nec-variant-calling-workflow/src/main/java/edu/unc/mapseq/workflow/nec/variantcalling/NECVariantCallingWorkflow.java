package edu.unc.mapseq.workflow.nec.variantcalling;

import java.io.File;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowPlan;
import edu.unc.mapseq.module.gatk.GATKDownsamplingType;
import edu.unc.mapseq.module.gatk.GATKPhoneHomeType;
import edu.unc.mapseq.module.gatk2.GATKDepthOfCoverageCLI;
import edu.unc.mapseq.module.gatk2.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroups;
import edu.unc.mapseq.module.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.samtools.SAMToolsFlagstatCLI;
import edu.unc.mapseq.module.samtools.SAMToolsIndexCLI;
import edu.unc.mapseq.workflow.AbstractWorkflow;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowJobFactory;
import edu.unc.mapseq.workflow.WorkflowUtil;

public class NECVariantCallingWorkflow extends AbstractWorkflow {

    private final Logger logger = LoggerFactory.getLogger(NECVariantCallingWorkflow.class);

    public NECVariantCallingWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return NECVariantCallingWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/nec/variantcalling/workflow");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.info("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");
        String depthOfCoverageIntervalList = getWorkflowBeanService().getAttributes()
                .get("depthOfCoverageIntervalList");
        String unifiedGenotyperIntervalList = getWorkflowBeanService().getAttributes().get(
                "unifiedGenotyperIntervalList");
        String unifiedGenotyperDBSNP = getWorkflowBeanService().getAttributes().get("unifiedGenotyperDBSNP");
        String GATKKey = getWorkflowBeanService().getAttributes().get("GATKKey");

        Set<HTSFSample> htsfSampleSet = getAggregateHTSFSampleSet();
        logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());

        Workflow alignmentWorkflow = null;
        try {
            alignmentWorkflow = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowDAO().findByName("NECAlignment");
        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
        }

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample,
                    getName().replace("VariantCalling", ""), getVersion());

            logger.info("htsfSample: {}", htsfSample.toString());
            Integer laneIndex = htsfSample.getLaneIndex();
            logger.debug("laneIndex = {}", laneIndex);
            Set<FileData> fileDataSet = htsfSample.getFileDatas();

            File bamFile = null;

            // 1st attempt to find bam file
            List<File> possibleVCFFileList = WorkflowUtil.lookupFileByJobAndMimeTypeAndWorkflowId(fileDataSet,
                    getWorkflowBeanService().getMaPSeqDAOBean(), PicardAddOrReplaceReadGroups.class,
                    MimeType.APPLICATION_BAM, alignmentWorkflow.getId());

            if (possibleVCFFileList != null && possibleVCFFileList.size() > 0) {
                bamFile = possibleVCFFileList.get(0);
            }

            String soughtAfterFileName = String.format("%s_%s_L%03d.fixed-rg.bam", sequencerRun.getName(),
                    htsfSample.getBarcode(), htsfSample.getLaneIndex());

            // 2nd attempt to find bam file
            if (bamFile == null) {
                logger.debug("looking for: {}", soughtAfterFileName);
                for (FileData fileData : fileDataSet) {
                    if (fileData.getName().equals(soughtAfterFileName)) {
                        bamFile = new File(fileData.getPath(), fileData.getName());
                        break;
                    }
                }
            }

            // 3rd attempt to find bam file
            if (bamFile == null) {
                logger.debug("still looking for: {}", soughtAfterFileName);
                for (File outputDirFile : outputDirectory.listFiles()) {
                    if (outputDirFile.getName().equals(soughtAfterFileName)) {
                        bamFile = outputDirFile;
                        break;
                    }
                }
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                throw new WorkflowException("bam file to process was not found");
            }

            if (bamFile != null && !bamFile.exists()) {
                logger.error("bam file doesn't exist");
                throw new WorkflowException("bam file doesn't exist");
            }

            try {

                // deduped job
                String dedupedBamFile = bamFile.getName().replace(".bam", ".deduped.bam");

                CondorJob dedupedBamJob = WorkflowJobFactory.createJob(++count, PicardMarkDuplicatesCLI.class,
                        getWorkflowPlan(), htsfSample);
                dedupedBamJob.setInitialDirectory(outputDirectory.getAbsolutePath());

                dedupedBamJob.addArgument(PicardMarkDuplicatesCLI.INPUT, bamFile.getName());
                dedupedBamJob.addTransferInput(bamFile.getName());

                dedupedBamJob.addArgument(PicardMarkDuplicatesCLI.OUTPUT, dedupedBamFile);
                dedupedBamJob.addTransferOutput(dedupedBamFile);

                String picardMarkDuplicatesMetricsFile = dedupedBamFile.replace(".bam", ".metrics");
                dedupedBamJob.addArgument(PicardMarkDuplicatesCLI.METRICSFILE, picardMarkDuplicatesMetricsFile);
                dedupedBamJob.addTransferOutput(picardMarkDuplicatesMetricsFile);

                graph.addVertex(dedupedBamJob);

                // index job
                CondorJob dedupedBaiJob = WorkflowJobFactory.createJob(++count, SAMToolsIndexCLI.class,
                        getWorkflowPlan(), htsfSample);
                dedupedBaiJob.setInitialDirectory(outputDirectory.getAbsolutePath());

                dedupedBaiJob.addArgument(SAMToolsIndexCLI.INPUT, dedupedBamFile);
                dedupedBaiJob.addTransferInput(dedupedBamFile);

                String dedupedBaiFile = dedupedBamFile.replace(".bam", ".bai");

                dedupedBaiJob.addArgument(SAMToolsIndexCLI.OUTPUT, dedupedBaiFile);
                dedupedBaiJob.addTransferOutput(dedupedBaiFile);

                graph.addVertex(dedupedBaiJob);
                graph.addEdge(dedupedBamJob, dedupedBaiJob);

                // flagstat job
                CondorJob dedupedRealignFixPrintReadsFlagstatJob = WorkflowJobFactory.createJob(++count,
                        SAMToolsFlagstatCLI.class, getWorkflowPlan(), htsfSample);
                dedupedRealignFixPrintReadsFlagstatJob.setInitialDirectory(outputDirectory.getAbsolutePath());

                dedupedRealignFixPrintReadsFlagstatJob.addArgument(SAMToolsFlagstatCLI.INPUT, dedupedBamFile);
                dedupedRealignFixPrintReadsFlagstatJob.addTransferInput(dedupedBamFile);

                String dedupedRealignFixPrintReadsFlagstatFile = dedupedBamFile.replace(".bam",
                        ".realign.fix.pr.flagstat");
                dedupedRealignFixPrintReadsFlagstatJob.addArgument(SAMToolsFlagstatCLI.OUTPUT,
                        dedupedRealignFixPrintReadsFlagstatFile);
                dedupedRealignFixPrintReadsFlagstatJob.addTransferOutput(dedupedRealignFixPrintReadsFlagstatFile);

                dedupedRealignFixPrintReadsFlagstatJob.addTransferInput(dedupedBaiFile);

                graph.addVertex(dedupedRealignFixPrintReadsFlagstatJob);
                graph.addEdge(dedupedBaiJob, dedupedRealignFixPrintReadsFlagstatJob);

                // depth of coverage job
                String dedupedRealignFixPrintReadsCoverageFile = dedupedBamFile.replace(".bam",
                        ".realign.fix.pr.coverage");
                CondorJob dedupedRealignFixPrintReadsCoverageJob = createGATKDepthOfCoverageJob(++count,
                        outputDirectory, getWorkflowPlan(), htsfSample, dedupedBamFile, dedupedBaiFile,
                        dedupedRealignFixPrintReadsCoverageFile, depthOfCoverageIntervalList, referenceSequence,
                        GATKKey);
                graph.addVertex(dedupedRealignFixPrintReadsCoverageJob);
                graph.addEdge(dedupedBaiJob, dedupedRealignFixPrintReadsCoverageJob);

                // unified genotyper job
                String dedupedRealignFixPrintReadsVcfFile = dedupedBamFile.replace(".bam", ".realign.fix.pr.vcf");
                CondorJob dedupedRealignFixPrintReadsVcfJob = createGATKUnifiedGenotyperJob(++count, outputDirectory,
                        getWorkflowPlan(), htsfSample, dedupedBamFile, dedupedBaiFile,
                        dedupedRealignFixPrintReadsVcfFile, unifiedGenotyperIntervalList, referenceSequence, GATKKey,
                        unifiedGenotyperDBSNP);
                graph.addVertex(dedupedRealignFixPrintReadsVcfJob);
                graph.addEdge(dedupedRealignFixPrintReadsCoverageJob, dedupedRealignFixPrintReadsVcfJob);

            } catch (Exception e) {
                throw new WorkflowException(e);
            }

        }

        return graph;
    }

    /**
     * create GATKDepthOfCoverage job
     * 
     * @param index
     * @param siteName
     * @param workFlowPlan
     * @param htsfSample
     * @param sourceFile
     * @param sourceFileIndex
     * @param destFile
     * @param intervalList
     * @param referenceSequence
     * @param GATKKey
     * @return
     */
    private CondorJob createGATKDepthOfCoverageJob(int index, File outputDirectory, WorkflowPlan workFlowPlan,
            HTSFSample htsfSample, String sourceFile, String sourceFileIndex, String destFile, String intervalList,
            String referenceSequence, String GATKKey) {

        // create new job - do not persist file data
        CondorJob job = WorkflowJobFactory
                .createJob(index, GATKDepthOfCoverageCLI.class, getWorkflowPlan(), htsfSample);

        // add source and destination files
        job.addArgument(GATKDepthOfCoverageCLI.INPUTFILE, sourceFile);
        job.addArgument(GATKDepthOfCoverageCLI.OUTPUTPREFIX, destFile);

        // add arguments
        job.addArgument(GATKDepthOfCoverageCLI.KEY, GATKKey);
        job.addArgument(GATKDepthOfCoverageCLI.REFERENCESEQUENCE, referenceSequence);
        job.addArgument(GATKDepthOfCoverageCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString());
        job.addArgument(GATKDepthOfCoverageCLI.DOWNSAMPLINGTYPE, GATKDownsamplingType.NONE.toString());
        job.addArgument(GATKDepthOfCoverageCLI.VALIDATIONSTRICTNESS, "LENIENT");
        job.addArgument(GATKDepthOfCoverageCLI.OMITDEPTHOUTPUTATEACHBASE);
        job.addArgument(GATKDepthOfCoverageCLI.INTERVALS, intervalList);

        job.setInitialDirectory(outputDirectory.getAbsolutePath());
        job.addTransferInput(sourceFile);
        job.addTransferInput(sourceFileIndex);

        job.addTransferOutput(String.format("%s.sample_cumulative_coverage_counts", destFile));
        job.addTransferOutput(String.format("%s.sample_cumulative_coverage_proportions", destFile));
        job.addTransferOutput(String.format("%s.sample_interval_statistics", destFile));
        job.addTransferOutput(String.format("%s.sample_interval_summary", destFile));
        job.addTransferOutput(String.format("%s.sample_statistics", destFile));
        job.addTransferOutput(String.format("%s.sample_summary", destFile));

        return job;
    }

    /**
     * create GATK UnifiedGenotyper job
     * 
     * @param index
     * @param siteName
     * @param workFlowPlan
     * @param htsfSample
     * @param sourceFile
     * @param sourceFileIndex
     * @param destFile
     * @param intervalList
     * @param referenceSequence
     * @param GATKKey
     * @param unifiedGenotyperDBSNP
     * @return
     */
    private CondorJob createGATKUnifiedGenotyperJob(int index, File outputDirectory, WorkflowPlan workFlowPlan,
            HTSFSample htsfSample, String sourceFile, String sourceFileIndex, String destFile, String intervalList,
            String referenceSequence, String GATKKey, String unifiedGenotyperDBSNP) {

        // create new job - do not persist file data
        CondorJob job = WorkflowJobFactory.createJob(index, GATKUnifiedGenotyperCLI.class, getWorkflowPlan(),
                htsfSample);

        // add source and destination files
        job.addArgument(GATKUnifiedGenotyperCLI.INPUTFILE, sourceFile);
        job.addArgument(GATKUnifiedGenotyperCLI.OUT, destFile);

        // add arguments
        job.addArgument(GATKUnifiedGenotyperCLI.KEY, GATKKey);
        job.addArgument(GATKUnifiedGenotyperCLI.INTERVALS, intervalList);
        job.addArgument(GATKUnifiedGenotyperCLI.REFERENCESEQUENCE, referenceSequence);
        job.addArgument(GATKUnifiedGenotyperCLI.DBSNP, unifiedGenotyperDBSNP);

        // set arguments
        job.addArgument(GATKUnifiedGenotyperCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString());
        job.addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLINGTYPE, GATKDownsamplingType.NONE.toString());
        job.addArgument(GATKUnifiedGenotyperCLI.GENOTYPELIKELIHOODSMODEL, "BOTH");
        job.addArgument(GATKUnifiedGenotyperCLI.OUTPUTMODE, "EMIT_ALL_SITES");
        job.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "AlleleBalance");
        job.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "DepthOfCoverage");
        job.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HomopolymerRun");
        job.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "MappingQualityZero");
        job.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "QualByDepth");
        job.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "RMSMappingQuality");
        job.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HaplotypeScore");
        job.addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLETOCOVERAGE, "250");
        job.addArgument(GATKUnifiedGenotyperCLI.STANDCALLCONF, "4");
        job.addArgument(GATKUnifiedGenotyperCLI.STANDEMITCONF, "0");

        // number of processors
        job.addArgument(GATKUnifiedGenotyperCLI.NUMTHREADS, "4");
        job.setNumberOfProcessors(4);

        // metrics file
        String gatkUnifiedGenotyperMetrics = sourceFile.replace(".bam", ".metrics");
        job.addArgument(GATKUnifiedGenotyperCLI.METRICS, gatkUnifiedGenotyperMetrics);

        job.setInitialDirectory(outputDirectory.getAbsolutePath());
        job.addTransferInput(sourceFile);
        job.addTransferInput(sourceFileIndex);
        job.addTransferOutput(gatkUnifiedGenotyperMetrics);
        job.addTransferOutput(destFile);

        return job;
    }

}
