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
import org.renci.jlrm.condor.CondorJobBuilder;
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
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowUtil;
import edu.unc.mapseq.workflow.impl.AbstractWorkflow;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;

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

                CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, PicardMarkDuplicatesCLI.class,
                        getWorkflowPlan(), htsfSample).initialDirectory(outputDirectory.getAbsolutePath());
                String picardMarkDuplicatesMetricsFile = dedupedBamFile.replace(".bam", ".metrics");
                builder.addArgument(PicardMarkDuplicatesCLI.INPUT, bamFile.getName())
                        .addArgument(PicardMarkDuplicatesCLI.OUTPUT, dedupedBamFile)
                        .addArgument(PicardMarkDuplicatesCLI.METRICSFILE, picardMarkDuplicatesMetricsFile);
                builder.addTransferInput(bamFile.getName()).addTransferOutput(dedupedBamFile)
                        .addTransferOutput(picardMarkDuplicatesMetricsFile);
                CondorJob dedupedBamJob = builder.build();
                logger.info(dedupedBamJob.toString());
                graph.addVertex(dedupedBamJob);

                // index job
                builder = WorkflowJobFactory.createJob(++count, SAMToolsIndexCLI.class, getWorkflowPlan(), htsfSample)
                        .initialDirectory(outputDirectory.getAbsolutePath());
                String dedupedBaiFile = dedupedBamFile.replace(".bam", ".bai");
                builder.addArgument(SAMToolsIndexCLI.INPUT, dedupedBamFile).addArgument(SAMToolsIndexCLI.OUTPUT,
                        dedupedBaiFile);
                builder.addTransferInput(dedupedBamFile).addTransferOutput(dedupedBaiFile);
                CondorJob dedupedBaiJob = builder.build();
                logger.info(dedupedBaiJob.toString());
                graph.addVertex(dedupedBaiJob);
                graph.addEdge(dedupedBamJob, dedupedBaiJob);

                // flagstat job
                builder = WorkflowJobFactory.createJob(++count, SAMToolsFlagstatCLI.class, getWorkflowPlan(),
                        htsfSample).initialDirectory(outputDirectory.getAbsolutePath());
                String dedupedRealignFixPrintReadsFlagstatFile = dedupedBamFile.replace(".bam",
                        ".realign.fix.pr.flagstat");
                builder.addArgument(SAMToolsFlagstatCLI.INPUT, dedupedBamFile).addArgument(SAMToolsFlagstatCLI.OUTPUT,
                        dedupedRealignFixPrintReadsFlagstatFile);
                builder.addTransferInput(dedupedBamFile).addTransferOutput(dedupedRealignFixPrintReadsFlagstatFile)
                        .addTransferInput(dedupedBaiFile);
                CondorJob dedupedRealignFixPrintReadsFlagstatJob = builder.build();
                logger.info(dedupedRealignFixPrintReadsFlagstatJob.toString());
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
        CondorJobBuilder builder = WorkflowJobFactory.createJob(index, GATKDepthOfCoverageCLI.class, getWorkflowPlan(),
                htsfSample).initialDirectory(outputDirectory.getAbsolutePath());

        // add source and destination files
        builder.addArgument(GATKDepthOfCoverageCLI.INPUTFILE, sourceFile)
                .addArgument(GATKDepthOfCoverageCLI.OUTPUTPREFIX, destFile)
                .addArgument(GATKDepthOfCoverageCLI.KEY, GATKKey)
                .addArgument(GATKDepthOfCoverageCLI.REFERENCESEQUENCE, referenceSequence)
                .addArgument(GATKDepthOfCoverageCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString())
                .addArgument(GATKDepthOfCoverageCLI.DOWNSAMPLINGTYPE, GATKDownsamplingType.NONE.toString())
                .addArgument(GATKDepthOfCoverageCLI.VALIDATIONSTRICTNESS, "LENIENT")
                .addArgument(GATKDepthOfCoverageCLI.OMITDEPTHOUTPUTATEACHBASE)
                .addArgument(GATKDepthOfCoverageCLI.INTERVALS, intervalList);

        builder.addTransferInput(sourceFile).addTransferInput(sourceFileIndex)
                .addTransferOutput(String.format("%s.sample_cumulative_coverage_counts", destFile))
                .addTransferOutput(String.format("%s.sample_cumulative_coverage_proportions", destFile))
                .addTransferOutput(String.format("%s.sample_interval_statistics", destFile))
                .addTransferOutput(String.format("%s.sample_interval_summary", destFile))
                .addTransferOutput(String.format("%s.sample_statistics", destFile))
                .addTransferOutput(String.format("%s.sample_summary", destFile));

        return builder.build();
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
        CondorJobBuilder builder = WorkflowJobFactory
                .createJob(index, GATKUnifiedGenotyperCLI.class, getWorkflowPlan(), htsfSample)
                .initialDirectory(outputDirectory.getAbsolutePath()).numberOfProcessors(4);
        String gatkUnifiedGenotyperMetrics = sourceFile.replace(".bam", ".metrics");

        // add source and destination files
        builder.addArgument(GATKUnifiedGenotyperCLI.INPUTFILE, sourceFile)
                .addArgument(GATKUnifiedGenotyperCLI.OUT, destFile).addArgument(GATKUnifiedGenotyperCLI.KEY, GATKKey)
                .addArgument(GATKUnifiedGenotyperCLI.INTERVALS, intervalList)
                .addArgument(GATKUnifiedGenotyperCLI.REFERENCESEQUENCE, referenceSequence)
                .addArgument(GATKUnifiedGenotyperCLI.DBSNP, unifiedGenotyperDBSNP)
                .addArgument(GATKUnifiedGenotyperCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString())
                .addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLINGTYPE, GATKDownsamplingType.NONE.toString())
                .addArgument(GATKUnifiedGenotyperCLI.GENOTYPELIKELIHOODSMODEL, "BOTH")
                .addArgument(GATKUnifiedGenotyperCLI.OUTPUTMODE, "EMIT_ALL_SITES")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "AlleleBalance")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "DepthOfCoverage")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HomopolymerRun")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "MappingQualityZero")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "QualByDepth")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "RMSMappingQuality")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HaplotypeScore")
                .addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLETOCOVERAGE, "250")
                .addArgument(GATKUnifiedGenotyperCLI.STANDCALLCONF, "4")
                .addArgument(GATKUnifiedGenotyperCLI.STANDEMITCONF, "0")
                .addArgument(GATKUnifiedGenotyperCLI.NUMTHREADS, "4")
                .addArgument(GATKUnifiedGenotyperCLI.METRICS, gatkUnifiedGenotyperMetrics);

        builder.addTransferInput(sourceFile).addTransferInput(sourceFileIndex)
                .addTransferOutput(gatkUnifiedGenotyperMetrics).addTransferOutput(destFile);

        return builder.build();
    }
}
