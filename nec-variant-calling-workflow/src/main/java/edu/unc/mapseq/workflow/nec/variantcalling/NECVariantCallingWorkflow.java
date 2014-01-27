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
import edu.unc.mapseq.module.gatk2.GATKBaseRecalibratorCLI;
import edu.unc.mapseq.module.gatk2.GATKDepthOfCoverageCLI;
import edu.unc.mapseq.module.gatk2.GATKIndelRealignerCLI;
import edu.unc.mapseq.module.gatk2.GATKPrintReadsCreatorCLI;
import edu.unc.mapseq.module.gatk2.GATKRealignerTargetCreatorCLI;
import edu.unc.mapseq.module.gatk2.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroups;
import edu.unc.mapseq.module.picard.PicardFixMateCLI;
import edu.unc.mapseq.module.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.picard.PicardSortOrderType;
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
            alignmentWorkflow = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowDAO()
                    .findByName("NIDAUCSFAlignment");
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
                CondorJob dedupedBamJob = createPicardMarkDuplicatesJob(++count, outputDirectory, getWorkflowPlan(),
                        htsfSample, bamFile.getName(), dedupedBamFile);
                graph.addVertex(dedupedBamJob);

                // index job
                String dedupedBaiFile = dedupedBamFile.replace(".bam", ".bai");
                CondorJob dedupedBaiJob = createSAMToolsIndexJob(++count, outputDirectory, getWorkflowPlan(),
                        htsfSample, dedupedBamFile, dedupedBaiFile);
                graph.addVertex(dedupedBaiJob);
                graph.addEdge(dedupedBamJob, dedupedBaiJob);

                /* REDUCED GATK WORKFLOW */

                // // realigner target creator job
                // String dedupedTargetsIntervalsFile = dedupedBamFile.replace(".bam", ".targets.intervals");
                // CondorJob dedupedTargetsIntervalsJob = createGATKRealignerTargetCreatorJob(++count, outputDirectory,
                // workflowPlan, htsfSample, dedupedBamFile, dedupedBaiFile, dedupedTargetsIntervalsFile,
                // referenceSequence, GATKKey, knownVCFList);
                // graph.addVertex(dedupedTargetsIntervalsJob);
                // graph.addEdge(dedupedBaiJob, dedupedTargetsIntervalsJob);
                //
                // // indel realigner target creator job
                // String dedupedRealignBamFile = dedupedBamFile.replace(".bam", ".realign.bam");
                // CondorJob dedupedRealignBamJob = createGATKIndelRealignerJob(++count, outputDirectory, workflowPlan,
                // htsfSample, dedupedBamFile, dedupedBaiFile, dedupedRealignBamFile, referenceSequence, GATKKey,
                // knownVCFList, dedupedTargetsIntervalsFile);
                // graph.addVertex(dedupedRealignBamJob);
                // graph.addEdge(dedupedTargetsIntervalsJob, dedupedRealignBamJob);
                //
                // // fix mate job
                // String dedupedRealignBaiFile = dedupedBamFile.replace(".bam", ".realign.bai");
                // String dedupedRealignFixBamFile = dedupedRealignBamFile.replace(".bam", ".fix.bam");
                // CondorJob dedupedRealignFixBamJob = createPicardFixMateJob(++count, outputDirectory, workflowPlan,
                // htsfSample, dedupedRealignBamFile, dedupedRealignBaiFile, dedupedRealignFixBamFile);
                // graph.addVertex(dedupedRealignFixBamJob);
                // graph.addEdge(dedupedRealignBamJob, dedupedRealignFixBamJob);
                //
                // // index job
                // String dedupedRealignFixBaiFile = dedupedRealignFixBamFile.replace(".bam", ".bai");
                // CondorJob dedupedRealignFixBaiJob = createSAMToolsIndexJob(++count, outputDirectory, workflowPlan,
                // htsfSample, dedupedRealignFixBamFile, dedupedRealignFixBaiFile);
                // graph.addVertex(dedupedRealignFixBaiJob);
                // graph.addEdge(dedupedRealignFixBamJob, dedupedRealignFixBaiJob);
                //
                // // Base recal job
                // String dedupedRealignFixRecalDataGrpFile = dedupedRealignFixBamFile.replace(".bam",
                // ".recal_data.grp");
                // CondorJob dedupedRealignFixRecalDataGrpJob = createGATKBaseRecalibratorJob(++count, outputDirectory,
                // workflowPlan, htsfSample, dedupedRealignFixBamFile, dedupedRealignFixBaiFile,
                // dedupedRealignFixRecalDataGrpFile, referenceSequence, GATKKey, knownSitesList);
                // graph.addVertex(dedupedRealignFixRecalDataGrpJob);
                // graph.addEdge(dedupedRealignFixBaiJob, dedupedRealignFixRecalDataGrpJob);
                //
                // // print reads job
                // String dedupedRealignFixPrintReadsBamFile = dedupedRealignFixBamFile.replace(".bam", ".pr.bam");
                // CondorJob dedupedRealignFixPrintReadsBamJob = createPrintReadsJob(++count, outputDirectory,
                // workflowPlan, htsfSample, dedupedRealignFixBamFile, dedupedRealignFixBaiFile,
                // dedupedRealignFixRecalDataGrpFile, dedupedRealignFixPrintReadsBamFile, referenceSequence,
                // GATKKey);
                // graph.addVertex(dedupedRealignFixPrintReadsBamJob);
                // graph.addEdge(dedupedRealignFixRecalDataGrpJob, dedupedRealignFixPrintReadsBamJob);
                //
                // // index job
                // String dedupedRealignFixPrintReadsBaiFile = dedupedRealignFixPrintReadsBamFile.replace(".bam",
                // ".bai");
                // CondorJob dedupedRealignFixPrintReadsBaiJob = createSAMToolsIndexJob(++count, outputDirectory,
                // workflowPlan, htsfSample, dedupedRealignFixPrintReadsBamFile,
                // dedupedRealignFixPrintReadsBaiFile);
                // graph.addVertex(dedupedRealignFixPrintReadsBaiJob);
                // graph.addEdge(dedupedRealignFixPrintReadsBamJob, dedupedRealignFixPrintReadsBaiJob);

                // Files and objects to adjust for reduced workflow
                CondorJob dedupedRealignFixPrintReadsBaiJob = dedupedBaiJob;
                String dedupedRealignFixPrintReadsBamFile = dedupedBamFile;
                String dedupedRealignFixPrintReadsBaiFile = dedupedBaiFile;

                // flagstat job
                // String dedupedRealignFixPrintReadsFlagstatFile = dedupedRealignFixPrintReadsBamFile.replace(".bam",
                // ".flagstat");
                String dedupedRealignFixPrintReadsFlagstatFile = dedupedRealignFixPrintReadsBamFile.replace(".bam",
                        ".realign.fix.pr.flagstat");
                CondorJob dedupedRealignFixPrintReadsFlagstatJob = createSAMToolsFlagstatJob(++count, outputDirectory,
                        getWorkflowPlan(), htsfSample, dedupedRealignFixPrintReadsBamFile,
                        dedupedRealignFixPrintReadsBaiFile, dedupedRealignFixPrintReadsFlagstatFile);
                graph.addVertex(dedupedRealignFixPrintReadsFlagstatJob);
                graph.addEdge(dedupedRealignFixPrintReadsBaiJob, dedupedRealignFixPrintReadsFlagstatJob);

                // depth of coverage job
                // String dedupedRealignFixPrintReadsCoverageFile = dedupedRealignFixPrintReadsBamFile.replace(".bam",
                // ".coverage");
                String dedupedRealignFixPrintReadsCoverageFile = dedupedRealignFixPrintReadsBamFile.replace(".bam",
                        ".realign.fix.pr.coverage");
                CondorJob dedupedRealignFixPrintReadsCoverageJob = createGATKDepthOfCoverageJob(++count,
                        outputDirectory, getWorkflowPlan(), htsfSample, dedupedRealignFixPrintReadsBamFile,
                        dedupedRealignFixPrintReadsBaiFile, dedupedRealignFixPrintReadsCoverageFile,
                        depthOfCoverageIntervalList, referenceSequence, GATKKey);
                graph.addVertex(dedupedRealignFixPrintReadsCoverageJob);
                graph.addEdge(dedupedRealignFixPrintReadsBaiJob, dedupedRealignFixPrintReadsCoverageJob);

                // unified genotyper job
                // String dedupedRealignFixPrintReadsVcfFile = dedupedRealignFixPrintReadsBamFile.replace(".bam",
                // ".vcf");
                String dedupedRealignFixPrintReadsVcfFile = dedupedRealignFixPrintReadsBamFile.replace(".bam",
                        ".realign.fix.pr.vcf");
                CondorJob dedupedRealignFixPrintReadsVcfJob = createGATKUnifiedGenotyperJob(++count, outputDirectory,
                        getWorkflowPlan(), htsfSample, dedupedRealignFixPrintReadsBamFile,
                        dedupedRealignFixPrintReadsBaiFile, dedupedRealignFixPrintReadsVcfFile,
                        unifiedGenotyperIntervalList, referenceSequence, GATKKey, unifiedGenotyperDBSNP);
                graph.addVertex(dedupedRealignFixPrintReadsVcfJob);
                graph.addEdge(dedupedRealignFixPrintReadsCoverageJob, dedupedRealignFixPrintReadsVcfJob);

            } catch (Exception e) {
                throw new WorkflowException(e);
            }

        }

        return graph;
    }

    /**
     * Create PicardMarkDuplicatesJob
     * 
     * @param index
     * @param siteName
     * @param workFlowPlan
     * @param htsfSample
     * @param sourceFile
     * @param destFile
     * @return
     */
    private CondorJob createPicardMarkDuplicatesJob(int index, File outputDirectory, WorkflowPlan workFlowPlan,
            HTSFSample htsfSample, String sourceFile, String destFile) {

        // create new job
        CondorJob job = WorkflowJobFactory.createJob(index, PicardMarkDuplicatesCLI.class, getWorkflowPlan(),
                htsfSample);

        // set input and output files
        job.addArgument(PicardMarkDuplicatesCLI.INPUT, sourceFile);
        job.addArgument(PicardMarkDuplicatesCLI.OUTPUT, destFile);

        // add parameters
        String picardMarkDuplicatesMetricsFile = sourceFile.replace(".bam", ".metrics");
        job.addArgument(PicardMarkDuplicatesCLI.METRICSFILE, picardMarkDuplicatesMetricsFile);

        job.setInitialDirectory(outputDirectory);
        job.addTransferInput(sourceFile);
        job.addTransferOutput(destFile);
        job.addTransferOutput(picardMarkDuplicatesMetricsFile);

        return job;
    }

    /**
     * Create SAMToolsIndex Job
     * 
     * @param index
     * @param siteName
     * @param workFlowPlan
     * @param htsfSample
     * @param sourceFile
     * @param destFile
     * @return
     */
    private CondorJob createSAMToolsIndexJob(int index, File outputDirectory, WorkflowPlan workFlowPlan,
            HTSFSample htsfSample, String sourceFile, String destFile) {

        // create new job - do not persist file data
        CondorJob job = WorkflowJobFactory.createJob(index, SAMToolsIndexCLI.class, workFlowPlan, htsfSample);

        // add source and destination files
        job.addArgument(SAMToolsIndexCLI.INPUT, sourceFile);
        job.addArgument(SAMToolsIndexCLI.OUTPUT, destFile);

        job.setInitialDirectory(outputDirectory);
        job.addTransferInput(sourceFile);
        job.addTransferOutput(destFile);

        return job;
    }

    /**
     * Create Realiger Target Creator job
     * 
     * @param index
     * @param siteName
     * @param workFlowPlan
     * @param htsfSample
     * @param sourceFile
     * @param sourceFileIndex
     * @param destFile
     * @param referenceSequence
     * @param GATKKey
     * @param knownVCFList
     * @return
     */
    private CondorJob createGATKRealignerTargetCreatorJob(int index, File outputDirectory, WorkflowPlan workFlowPlan,
            HTSFSample htsfSample, String sourceFile, String sourceFileIndex, String destFile,
            String referenceSequence, String GATKKey, List<String> knownVCFList) {

        // create new job - do not persist file data
        CondorJob job = WorkflowJobFactory.createJob(index, GATKRealignerTargetCreatorCLI.class, getWorkflowPlan(),
                htsfSample);

        // add source and destination files
        job.addArgument(GATKRealignerTargetCreatorCLI.INPUTFILE, sourceFile);
        job.addArgument(GATKRealignerTargetCreatorCLI.OUT, destFile);

        // add arguments
        job.addArgument(GATKRealignerTargetCreatorCLI.REFERENCESEQUENCE, referenceSequence);
        job.addArgument(GATKRealignerTargetCreatorCLI.KEY, GATKKey);
        job.addArgument(GATKRealignerTargetCreatorCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString());
        job.addArgument(GATKRealignerTargetCreatorCLI.DOWNSAMPLINGTYPE, GATKDownsamplingType.NONE.toString());

        // add all vcf files
        for (String known : knownVCFList) {
            job.addArgument(GATKRealignerTargetCreatorCLI.KNOWN, known);
        }

        job.setInitialDirectory(outputDirectory);
        job.addTransferInput(sourceFile);
        job.addTransferInput(sourceFileIndex);
        job.addTransferOutput(destFile);

        return job;
    }

    /**
     * create Indel Relaligner job
     * 
     * @param index
     * @param siteName
     * @param workFlowPlan
     * @param htsfSample
     * @param sourceFile
     * @param sourceFileIndex
     * @param destFile
     * @param referenceSequence
     * @param GATKKey
     * @param knownVCFList
     * @param targetsSourceFile
     * @return
     */
    private CondorJob createGATKIndelRealignerJob(int index, File outputDirectory, WorkflowPlan workFlowPlan,
            HTSFSample htsfSample, String sourceFile, String sourceFileIndex, String destFile,
            String referenceSequence, String GATKKey, List<String> knownVCFList, String targetsSourceFile) {

        // create new job - do not persist file data
        CondorJob job = WorkflowJobFactory.createJob(index, GATKIndelRealignerCLI.class, getWorkflowPlan(), htsfSample);

        // add source and destination files
        job.addArgument(GATKIndelRealignerCLI.INPUT, sourceFile);
        job.addArgument(GATKIndelRealignerCLI.OUT, destFile);
        job.addArgument(GATKIndelRealignerCLI.TARGETINTERVALS, targetsSourceFile);

        // add arguments
        job.addArgument(GATKIndelRealignerCLI.REFERENCESEQUENCE, referenceSequence);
        job.addArgument(GATKIndelRealignerCLI.KEY, GATKKey);
        job.addArgument(GATKIndelRealignerCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString());
        job.addArgument(GATKIndelRealignerCLI.DOWNSAMPLINGTYPE, GATKDownsamplingType.NONE.toString());

        // add all vcf files
        for (String known : knownVCFList) {
            job.addArgument(GATKIndelRealignerCLI.KNOWN, known);
        }

        job.setInitialDirectory(outputDirectory);
        job.addTransferInput(sourceFile);
        job.addTransferInput(sourceFileIndex);
        job.addTransferInput(targetsSourceFile);
        job.addTransferOutput(destFile);

        return job;
    }

    /**
     * Create Picard Fix Mate Job
     * 
     * @param index
     * @param siteName
     * @param workFlowPlan
     * @param htsfSample
     * @param sourceFile
     * @param sourceFileIndex
     * @param destFile
     * @return
     */
    private CondorJob createPicardFixMateJob(int index, File outputDirectory, WorkflowPlan workFlowPlan,
            HTSFSample htsfSample, String sourceFile, String sourceFileIndex, String destFile) {

        // create new job - do not persist file data
        CondorJob job = WorkflowJobFactory.createJob(index, PicardFixMateCLI.class, workFlowPlan, htsfSample);

        // add source and destination files
        job.addArgument(PicardFixMateCLI.INPUT, sourceFile);
        job.addArgument(PicardFixMateCLI.OUTPUT, destFile);

        // add arguments
        job.addArgument(PicardFixMateCLI.SORTORDER, PicardSortOrderType.COORDINATE.toString().toLowerCase());

        job.setInitialDirectory(outputDirectory);
        job.addTransferInput(sourceFile);
        job.addTransferInput(sourceFileIndex);
        job.addTransferOutput(destFile);

        return job;
    }

    /**
     * create Base Recal job
     * 
     * @param index
     * @param siteName
     * @param workFlowPlan
     * @param htsfSample
     * @param sourceFile
     * @param sourceFileIndex
     * @param destFile
     * @param referenceSequence
     * @param GATKKey
     * @param knownSitesList
     * @return
     */
    private CondorJob createGATKBaseRecalibratorJob(int index, File outputDirectory, WorkflowPlan workFlowPlan,
            HTSFSample htsfSample, String sourceFile, String sourceFileIndex, String destFile,
            String referenceSequence, String GATKKey, List<String> knownSitesList) {

        // create new job - do not persist file data
        CondorJob job = WorkflowJobFactory.createJob(index, GATKBaseRecalibratorCLI.class, getWorkflowPlan(),
                htsfSample);

        // add source and destination files
        job.addArgument(GATKBaseRecalibratorCLI.INPUTFILE, sourceFile);
        job.addArgument(GATKBaseRecalibratorCLI.OUT, destFile);

        // add arguments
        job.addArgument(GATKBaseRecalibratorCLI.REFERENCESEQUENCE, referenceSequence);
        job.addArgument(GATKBaseRecalibratorCLI.KEY, GATKKey);
        job.addArgument(GATKBaseRecalibratorCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString());

        // add all known sites
        for (String known : knownSitesList) {
            job.addArgument(GATKBaseRecalibratorCLI.KNOWNSITES, known);
        }

        job.setInitialDirectory(outputDirectory);
        job.addTransferInput(sourceFile);
        job.addTransferInput(sourceFileIndex);
        job.addTransferOutput(destFile);

        return job;
    }

    /**
     * create a new Print Reads Job
     * 
     * @param index
     * @param siteName
     * @param workFlowPlan
     * @param htsfSample
     * @param sourceFile
     * @param sourceFileIndex
     * @param recalFile
     * @param destFile
     * @param referenceSequence
     * @param GATKKey
     * @return
     */
    private CondorJob createPrintReadsJob(int index, File outputDirectory, WorkflowPlan workFlowPlan,
            HTSFSample htsfSample, String sourceFile, String sourceFileIndex, String recalFile, String destFile,
            String referenceSequence, String GATKKey) {

        // create new job - do not persist file data
        CondorJob job = WorkflowJobFactory.createJob(index, GATKPrintReadsCreatorCLI.class, getWorkflowPlan(),
                htsfSample);

        // add source and destination files
        job.addArgument(GATKPrintReadsCreatorCLI.INPUTFILE, sourceFile);
        job.addArgument(GATKPrintReadsCreatorCLI.OUT, destFile);

        // add arguments
        job.addArgument(GATKPrintReadsCreatorCLI.KEY, GATKKey);
        job.addArgument(GATKPrintReadsCreatorCLI.REFERENCESEQUENCE, referenceSequence);
        job.addArgument(GATKPrintReadsCreatorCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString());
        job.addArgument(GATKPrintReadsCreatorCLI.BASEQUALITYSCORERECALIBRATIONFILE, recalFile);

        job.setInitialDirectory(outputDirectory);
        job.addTransferInput(sourceFile);
        job.addTransferInput(sourceFileIndex);
        job.addTransferInput(recalFile);
        job.addTransferOutput(destFile);

        return job;
    }

    /**
     * create SAMTools flagstat job
     * 
     * @param index
     * @param siteName
     * @param workFlowPlan
     * @param htsfSample
     * @param sourceFile
     * @param sourceFileIndex
     * @param destFile
     * @return
     */
    private CondorJob createSAMToolsFlagstatJob(int index, File outputDirectory, WorkflowPlan workFlowPlan,
            HTSFSample htsfSample, String sourceFile, String sourceFileIndex, String destFile) {

        // create new job - do not persist file data
        CondorJob job = WorkflowJobFactory.createJob(index, SAMToolsFlagstatCLI.class, getWorkflowPlan(), htsfSample);

        // add source and destination files
        job.addArgument(SAMToolsFlagstatCLI.INPUT, sourceFile);
        job.addArgument(SAMToolsFlagstatCLI.OUTPUT, destFile);

        job.setInitialDirectory(outputDirectory);
        job.addTransferInput(sourceFile);
        job.addTransferInput(sourceFileIndex);
        job.addTransferOutput(destFile);

        return job;
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

        job.setInitialDirectory(outputDirectory);
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

        job.setInitialDirectory(outputDirectory);
        job.addTransferInput(sourceFile);
        job.addTransferInput(sourceFileIndex);
        job.addTransferOutput(gatkUnifiedGenotyperMetrics);
        job.addTransferOutput(destFile);

        return job;
    }

}
