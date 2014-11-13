package edu.unc.mapseq.commons.nec.variantcalling;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Sample;

public class SaveDepthOfCoverageAttributesRunnable implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(SaveDepthOfCoverageAttributesRunnable.class);

    private Long sampleId;

    private Long flowcellId;

    private MaPSeqDAOBean mapseqDAOBean;

    @Override
    public void run() {
        logger.info("ENTERING doExecute()");

        Set<Sample> sampleSet = new HashSet<Sample>();

        SampleDAO sampleDAO = mapseqDAOBean.getSampleDAO();
        try {
            if (flowcellId != null) {
                sampleSet.addAll(sampleDAO.findByFlowcellId(flowcellId));
            }

            if (sampleId != null) {
                Sample sample = sampleDAO.findById(sampleId);
                if (sample == null) {
                    logger.error("Sample was not found");
                    return;
                }
                sampleSet.add(sample);
            }
        } catch (MaPSeqDAOException e) {
            logger.warn("MaPSeqDAOException", e);
        }

        for (Sample sample : sampleSet) {

            File outputDirectory = new File(sample.getOutputDirectory(), "NECVariantCalling");

            if (!outputDirectory.exists()) {
                continue;
            }

            Set<Attribute> attributeSet = sample.getAttributes();

            Set<String> attributeNameSet = new HashSet<String>();

            for (Attribute attribute : attributeSet) {
                attributeNameSet.add(attribute.getName());
            }

            Set<String> synchSet = Collections.synchronizedSet(attributeNameSet);

            List<File> files = Arrays.asList(outputDirectory.listFiles());

            if (files == null || (files != null && files.isEmpty())) {
                logger.warn("no files found");
                continue;
            }

            File sampleSummaryFile = null;
            for (File f : files) {
                if (f.getName().endsWith(".coverage.sample_summary")) {
                    sampleSummaryFile = f;
                    break;
                }
            }

            if (sampleSummaryFile != null && sampleSummaryFile.exists()) {
                List<String> lines = null;
                try {
                    lines = FileUtils.readLines(sampleSummaryFile);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (lines != null) {
                    for (String line : lines) {
                        if (line.contains("Total")) {
                            String[] split = StringUtils.split(line);

                            if (synchSet.contains("GATKDepthOfCoverage.totalCoverage")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("GATKDepthOfCoverage.totalCoverage")) {
                                        attribute.setValue(split[1]);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new Attribute("GATKDepthOfCoverage.totalCoverage", split[1]));
                            }

                            if (synchSet.contains("GATKDepthOfCoverage.mean")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("GATKDepthOfCoverage.mean")) {
                                        attribute.setValue(split[1]);
                                        break;
                                    }
                                }
                            } else {
                                attributeSet.add(new Attribute("GATKDepthOfCoverage.mean", split[2]));
                            }
                        }
                    }
                }

                File sampleIntervalSummaryFile = null;
                for (File f : files) {
                    if (f.getName().endsWith(".coverage.sample_interval_summary")) {
                        sampleIntervalSummaryFile = f;
                        break;
                    }
                }

                if (sampleIntervalSummaryFile != null && sampleIntervalSummaryFile.exists()) {

                    long totalCoverageCount = 0;
                    BufferedReader br = null;
                    try {
                        br = new BufferedReader(new FileReader(sampleIntervalSummaryFile));
                        String line;
                        br.readLine();
                        while ((line = br.readLine()) != null) {
                            totalCoverageCount += Long.valueOf(StringUtils.split(line)[1].trim());
                        }
                    } catch (NumberFormatException | IOException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            br.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    if (synchSet.contains("GATKDepthOfCoverage.totalCoverageCount")) {
                        for (Attribute attribute : attributeSet) {
                            if (attribute.getName().equals("GATKDepthOfCoverage.totalCoverageCount")) {
                                attribute.setValue(totalCoverageCount + "");
                                break;
                            }
                        }
                    } else {
                        attributeSet.add(new Attribute("GATKDepthOfCoverage.totalCoverageCount", totalCoverageCount
                                + ""));
                    }

                    Long totalPassedReads = null;
                    for (Attribute attribute : attributeSet) {
                        if ("SAMToolsFlagstat.totalPassedReads".equals(attribute.getName())) {
                            totalPassedReads = Long.valueOf(attribute.getValue());
                            break;
                        }
                    }

                    if (totalPassedReads != null) {
                        if (synchSet.contains("numberOnTarget")) {
                            for (Attribute attribute : attributeSet) {
                                if (attribute.getName().equals("numberOnTarget") && totalPassedReads != null) {
                                    attribute.setValue((double) totalCoverageCount / (totalPassedReads * 100) + "");
                                    break;
                                }
                            }
                        } else {
                            attributeSet.add(new Attribute("numberOnTarget", (double) totalCoverageCount
                                    / (totalPassedReads * 100) + ""));
                        }
                    }

                }

            }

            try {
                sample.setAttributes(attributeSet);
                sampleDAO.save(sample);
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }

        }

    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public MaPSeqDAOBean getMapseqDAOBean() {
        return mapseqDAOBean;
    }

    public void setMapseqDAOBean(MaPSeqDAOBean mapseqDAOBean) {
        this.mapseqDAOBean = mapseqDAOBean;
    }

}
