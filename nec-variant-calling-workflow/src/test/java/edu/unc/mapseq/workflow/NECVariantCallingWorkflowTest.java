package edu.unc.mapseq.workflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jgrapht.DirectedGraph;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.ext.CondorDOTExporter;

import edu.unc.mapseq.module.gatk2.GATKBaseRecalibratorCLI;
import edu.unc.mapseq.module.gatk2.GATKDepthOfCoverageCLI;
import edu.unc.mapseq.module.gatk2.GATKIndelRealignerCLI;
import edu.unc.mapseq.module.gatk2.GATKPrintReadsCreatorCLI;
import edu.unc.mapseq.module.gatk2.GATKRealignerTargetCreatorCLI;
import edu.unc.mapseq.module.gatk2.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.picard.PicardFixMateCLI;
import edu.unc.mapseq.module.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.samtools.SAMToolsFlagstatCLI;
import edu.unc.mapseq.module.samtools.SAMToolsIndexCLI;

public class NECVariantCallingWorkflowTest {

    @Test
    public void createDot() {

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        // new job
        CondorJob picardMarkDuplicatesJob = new CondorJobBuilder().name(
                String.format("%s_%d", PicardMarkDuplicatesCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardMarkDuplicatesJob);

        // new job
        CondorJob samtoolsIndexJob = new CondorJobBuilder().name(
                String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

        // new job
        CondorJob gatkRealignTargetCreatorJob = new CondorJobBuilder().name(
                String.format("%s_%d", GATKRealignerTargetCreatorCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkRealignTargetCreatorJob);
        graph.addEdge(samtoolsIndexJob, gatkRealignTargetCreatorJob);

        // new job
        CondorJob gatkIndelRealignerJob = new CondorJobBuilder().name(
                String.format("%s_%d", GATKIndelRealignerCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkIndelRealignerJob);
        graph.addEdge(gatkRealignTargetCreatorJob, gatkIndelRealignerJob);

        // new job
        CondorJob picardFixMateJob = new CondorJobBuilder().name(
                String.format("%s_%d", PicardFixMateCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardFixMateJob);
        graph.addEdge(gatkIndelRealignerJob, picardFixMateJob);

        // new job
        samtoolsIndexJob = new CondorJobBuilder().name(
                String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardFixMateJob, samtoolsIndexJob);

        // new job
        CondorJob gatkBaseRecalibratorJob = new CondorJobBuilder().name(
                String.format("%s_%d", GATKBaseRecalibratorCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkBaseRecalibratorJob);
        graph.addEdge(samtoolsIndexJob, gatkBaseRecalibratorJob);

        // new job
        CondorJob gatkPrintReadsJob = new CondorJobBuilder().name(
                String.format("%s_%d", GATKPrintReadsCreatorCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkPrintReadsJob);
        graph.addEdge(gatkBaseRecalibratorJob, gatkPrintReadsJob);

        // new job
        samtoolsIndexJob = new CondorJobBuilder().name(
                String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(gatkPrintReadsJob, samtoolsIndexJob);

        // new job
        CondorJob samtoolsFlagstatJob = new CondorJobBuilder().name(
                String.format("%s_%d", SAMToolsFlagstatCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsFlagstatJob);
        graph.addEdge(samtoolsIndexJob, samtoolsFlagstatJob);

        // new job
        CondorJob gatkDepthOfCoverageJob = new CondorJobBuilder().name(
                String.format("%s_%d", GATKDepthOfCoverageCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkDepthOfCoverageJob);
        graph.addEdge(samtoolsIndexJob, gatkDepthOfCoverageJob);

        // new job
        CondorJob gatkUnifiedGenotyperJob = new CondorJobBuilder().name(
                String.format("%s_%d", GATKUnifiedGenotyperCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkUnifiedGenotyperJob);
        graph.addEdge(gatkDepthOfCoverageJob, gatkUnifiedGenotyperJob);

        VertexNameProvider<CondorJob> vnpId = new VertexNameProvider<CondorJob>() {
            @Override
            public String getVertexName(CondorJob job) {
                return job.getName();
            }
        };

        VertexNameProvider<CondorJob> vnpLabel = new VertexNameProvider<CondorJob>() {
            @Override
            public String getVertexName(CondorJob job) {
                return job.getName();
            }
        };

        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(
                vnpId, vnpLabel, null, null, null, null);
        File srcSiteResourcesImagesDir = new File("src/site/resources/images");
        if (!srcSiteResourcesImagesDir.exists()) {
            srcSiteResourcesImagesDir.mkdirs();
        }
        File dotFile = new File(srcSiteResourcesImagesDir, "workflow.dag.dot");
        try {
            FileWriter fw = new FileWriter(dotFile);
            dotExporter.export(fw, graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
