package edu.unc.mapseq.commons.nec.variantcalling;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.AttributeDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;

public class SaveFlagstatAttributesRunnable implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(SaveFlagstatAttributesRunnable.class);

    private Long sampleId;

    private Long flowcellId;

    private MaPSeqDAOBean mapseqDAOBean;

    @Override
    public void run() {
        logger.info("ENTERING run()");

        Set<Sample> sampleSet = new HashSet<Sample>();

        SampleDAO sampleDAO = mapseqDAOBean.getSampleDAO();
        AttributeDAO attributeDAO = mapseqDAOBean.getAttributeDAO();

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

        Workflow ncgenesWorkflow = null;
        try {
            WorkflowDAO workflowDAO = mapseqDAOBean.getWorkflowDAO();
            List<Workflow> workflowList = workflowDAO.findByName("NECVariantCalling");
            if (workflowList != null && !workflowList.isEmpty()) {
                ncgenesWorkflow = workflowList.get(0);
            }
        } catch (MaPSeqDAOException e2) {
            logger.error("Error", e2);
        }

        if (ncgenesWorkflow == null) {
            logger.error("NECVariantCalling workflow not found");
            return;
        }

        for (Sample sample : sampleSet) {

            logger.info(sample.toString());

            File outputDirectory = new File(sample.getOutputDirectory(), "NECVariantCalling");

            Set<Attribute> attributeSet = sample.getAttributes();
            Set<FileData> sampleFileDataSet = sample.getFileDatas();

            File flagstatFile = null;

            for (FileData fileData : sampleFileDataSet) {
                if (MimeType.TEXT_STAT_SUMMARY.equals(fileData.getMimeType())
                        && fileData.getName().endsWith(".flagstat")) {
                    flagstatFile = new File(fileData.getPath(), fileData.getName());
                    break;
                }
            }

            if (flagstatFile == null) {
                logger.error("flagstat file to process was not found...checking FS");
                if (outputDirectory.exists()) {
                    File[] files = outputDirectory.listFiles();
                    if (files != null && files.length > 0) {
                        for (File file : files) {
                            if (file.getName().endsWith("samtools.flagstat")) {
                                flagstatFile = file;
                                break;
                            }
                        }
                    }
                }
            }

            if (flagstatFile == null) {
                logger.error("flagstat file to process was still not found");
                continue;
            }

            logger.info("flagstat file is: {}", flagstatFile.getAbsolutePath());

            List<String> lines = null;
            try {
                lines = FileUtils.readLines(flagstatFile);
            } catch (IOException e1) {
                e1.printStackTrace();
            }

            Set<String> attributeNameSet = new HashSet<String>();

            for (Attribute attribute : attributeSet) {
                attributeNameSet.add(attribute.getName());
            }

            Set<String> synchSet = Collections.synchronizedSet(attributeNameSet);

            if (lines != null) {

                for (String line : lines) {

                    if (line.contains("in total")) {
                        String value = line.substring(0, line.indexOf(" ")).trim();
                        if (synchSet.contains("SAMToolsFlagstat.totalPassedReads")) {
                            for (Attribute attribute : attributeSet) {
                                if (attribute.getName().equals("SAMToolsFlagstat.totalPassedReads")) {
                                    attribute.setValue(value);
                                    try {
                                        attributeDAO.save(attribute);
                                    } catch (MaPSeqDAOException e) {
                                        logger.error("MaPSeqDAOException", e);
                                    }
                                    break;
                                }
                            }
                        } else {
                            attributeSet.add(new Attribute("SAMToolsFlagstat.totalPassedReads", value));
                        }
                    }

                    if (line.contains("mapped (")) {
                        Pattern pattern = Pattern.compile("^.+\\((.+)\\)");
                        Matcher matcher = pattern.matcher(line);
                        if (matcher.matches()) {
                            String value = matcher.group(1);
                            value = value.substring(0, value.indexOf("%")).trim();
                            if (StringUtils.isNotEmpty(value)) {
                                if (synchSet.contains("SAMToolsFlagstat.aligned")) {
                                    for (Attribute attribute : attributeSet) {
                                        if (attribute.getName().equals("SAMToolsFlagstat.aligned")) {
                                            attribute.setValue(value);
                                            break;
                                        }
                                    }
                                } else {
                                    attributeSet.add(new Attribute("SAMToolsFlagstat.aligned", value));
                                }
                            }
                        }
                    }

                    if (line.contains("properly paired (")) {
                        Pattern pattern = Pattern.compile("^.+\\((.+)\\)");
                        Matcher matcher = pattern.matcher(line);
                        if (matcher.matches()) {
                            String value = matcher.group(1);
                            value = value.substring(0, value.indexOf("%"));
                            if (StringUtils.isNotEmpty(value)) {
                                if (synchSet.contains("SAMToolsFlagstat.paired")) {
                                    for (Attribute attribute : attributeSet) {
                                        if (attribute.getName().equals("SAMToolsFlagstat.paired")) {
                                            attribute.setValue(value);
                                            break;
                                        }
                                    }
                                } else {
                                    attributeSet.add(new Attribute("SAMToolsFlagstat.paired", value));
                                }
                            }
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
            logger.info("DONE");

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
