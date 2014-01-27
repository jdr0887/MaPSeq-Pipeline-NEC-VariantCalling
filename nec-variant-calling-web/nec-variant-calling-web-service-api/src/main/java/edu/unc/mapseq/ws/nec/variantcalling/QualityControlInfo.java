package edu.unc.mapseq.ws.nec.variantcalling;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.renci.vcf.VCFResult;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "QualityControlResults", propOrder = {})
@XmlRootElement(name = "qualityControlResults")
public class QualityControlInfo {

    private Integer passedReads;

    private Float aligned;

    private Float paired;

    private Long totalCoverage;

    private Double mean;

    @XmlElement(name = "ICSNPResult")
    private VCFResult icSNPResultList;

    public QualityControlInfo() {
        super();
    }

    public Float getAligned() {
        return aligned;
    }

    public void setAligned(Float aligned) {
        this.aligned = aligned;
    }

    public Float getPaired() {
        return paired;
    }

    public void setPaired(Float paired) {
        this.paired = paired;
    }

    public Long getTotalCoverage() {
        return totalCoverage;
    }

    public void setTotalCoverage(Long totalCoverage) {
        this.totalCoverage = totalCoverage;
    }

    public Double getMean() {
        return mean;
    }

    public void setMean(Double mean) {
        this.mean = mean;
    }

    public VCFResult getIcSNPResultList() {
        return icSNPResultList;
    }

    public void setIcSNPResultList(VCFResult icSNPResultList) {
        this.icSNPResultList = icSNPResultList;
    }

    public Integer getPassedReads() {
        return passedReads;
    }

    public void setPassedReads(Integer passedReads) {
        this.passedReads = passedReads;
    }

    @Override
    public String toString() {
        return "QualityControlResults [passedReads=" + passedReads + ", aligned=" + aligned + ", paired=" + paired
                + ", totalCoverage=" + totalCoverage + ", mean=" + mean + ", icSNPResultList=" + icSNPResultList + "]";
    }

}
