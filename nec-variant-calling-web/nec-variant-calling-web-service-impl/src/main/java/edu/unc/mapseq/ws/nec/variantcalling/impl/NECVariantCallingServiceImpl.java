package edu.unc.mapseq.ws.nec.variantcalling.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.ws.nec.variantcalling.NECVariantCallingService;
import edu.unc.mapseq.ws.nec.variantcalling.QualityControlInfo;

public class NECVariantCallingServiceImpl implements NECVariantCallingService {

    private final Logger logger = LoggerFactory.getLogger(NECVariantCallingServiceImpl.class);

    private SampleDAO sampleDAO;

    @Override
    public QualityControlInfo lookupQuantificationResults(Long sampleId) {
        logger.debug("ENTERING lookupQuantificationResults(Long)");
        Sample sample = null;
        if (sampleId == null) {
            logger.warn("htsfSampleId is null");
            return null;
        }

        try {
            sample = sampleDAO.findById(sampleId);
            logger.info(sample.toString());
        } catch (MaPSeqDAOException e) {
            logger.error("Failed to find Sample", e);
        }

        if (sample == null) {
            return null;
        }

        Set<FileData> sampleFileDataSet = sample.getFileDatas();

        QualityControlInfo ret = new QualityControlInfo();

        if (sampleFileDataSet != null) {

            for (FileData fileData : sampleFileDataSet) {

                if (MimeType.TEXT_STAT_SUMMARY.equals(fileData.getMimeType())
                        && fileData.getName().endsWith(".flagstat")) {
                    File flagstatFile = new File(fileData.getPath(), fileData.getName());
                    logger.info("flagstat file is: {}", flagstatFile.getAbsolutePath());
                    if (flagstatFile.exists()) {
                        List<String> lines = null;
                        try {
                            lines = FileUtils.readLines(flagstatFile);
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }

                        if (lines != null) {
                            for (String line : lines) {

                                if (line.contains("in total")) {
                                    String value = line.substring(0, line.indexOf(" ")).trim();
                                    try {
                                        ret.setPassedReads(Integer.valueOf(value));
                                    } catch (Exception e) {
                                        logger.error("problem getting passedReads, value: {}", value);
                                    }
                                }

                                if (line.contains("mapped (")) {
                                    Pattern pattern = Pattern.compile("^.+\\((.+)\\)");
                                    Matcher matcher = pattern.matcher(line);
                                    if (matcher.matches()) {
                                        String value = matcher.group(1);
                                        value = value.substring(0, value.indexOf("%")).trim();
                                        if (StringUtils.isNotEmpty(value)) {
                                            try {
                                                ret.setAligned(Float.valueOf(value));
                                            } catch (Exception e) {
                                                logger.error("problem getting mapped, value: {}", value);
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
                                            try {
                                                ret.setPaired(Float.valueOf(value));
                                            } catch (Exception e) {
                                                logger.error("problem getting paired, value: {}", value);
                                            }
                                        }
                                    }
                                }
                            }

                        }

                    }
                }

                if (MimeType.TEXT_DEPTH_OF_COVERAGE_SUMMARY.equals(fileData.getMimeType())
                        && !fileData.getName().contains("gene")) {
                    File depthOfCoverageSummaryFile = new File(fileData.getPath(), fileData.getName());
                    logger.info("depthOfCoverageSummaryFile file is: {}", depthOfCoverageSummaryFile.getAbsolutePath());

                    if (!depthOfCoverageSummaryFile.exists()) {
                        logger.warn("depthOfCoverageSummaryFile doesn't exist");
                    } else {

                        try {
                            List<String> lines = FileUtils.readLines(depthOfCoverageSummaryFile);
                            for (String line : lines) {
                                if (line.contains("Total")) {
                                    String[] split = line.split("\t");
                                    ret.setTotalCoverage(Long.valueOf(split[1]));
                                    ret.setMean(Double.valueOf(split[2]));
                                }
                            }
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }

                }
            }

        }

        return ret;
    }

    @Override
    public List<QualityControlInfo> lookupQuantificationResultsBySequencerRun(Long flowcellId) {
        List<QualityControlInfo> ret = new ArrayList<QualityControlInfo>();
        try {
            List<Sample> sampleList = sampleDAO.findByFlowcellId(flowcellId);
            if (sampleList != null) {
                for (Sample sample : sampleList) {
                    Long htsfSampleId = sample.getId();
                    ret.add(lookupQuantificationResults(htsfSampleId));
                }
            }
        } catch (MaPSeqDAOException e) {
            logger.error("MaPSeqDAOException", e);
        }
        return ret;
    }

    public SampleDAO getSampleDAO() {
        return sampleDAO;
    }

    public void setSampleDAO(SampleDAO sampleDAO) {
        this.sampleDAO = sampleDAO;
    }

}
