package edu.unc.mapseq.messaging;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.ws.SampleService;

public class NECVariantCallingMessageTest {

    @Test
    public void testQueue() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                "biodev2.its.unc.edu"));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/nec.variantcalling");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            String format = "{\"entities\":[{\"entity_type\":\"Sample\",\"guid\":\"%d\"},{\"entity_type\":\"WorkflowRun\",\"name\":\"%s-%d\"}]}";
            producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 67401,
                    "jdr-test-nec-variant-calling", 67401)));
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testStressQueue() {
        QName serviceQName = new QName("http://ws.mapseq.unc.edu", "SampleService");
        QName portQName = new QName("http://ws.mapseq.unc.edu", "SamplePort");
        Service service = Service.create(serviceQName);
        String host = "biodev2.its.unc.edu";
        service.addPort(portQName, SOAPBinding.SOAP11HTTP_MTOM_BINDING,
                String.format("http://%s:%d/cxf/SampleService", host, 8181));
        SampleService htsfSampleService = service.getPort(SampleService.class);

        List<Sample> sampleList = new ArrayList<Sample>();

        sampleList.addAll(htsfSampleService.findByFlowcellId(191541L));
        sampleList.addAll(htsfSampleService.findByFlowcellId(191738L));
        // sampleList.addAll(htsfSampleService.findBySequencerRunId(190345L));
        // sampleList.addAll(htsfSampleService.findBySequencerRunId(190520L));
        // sampleList.addAll(htsfSampleService.findBySequencerRunId(191372L));
        // sampleList.addAll(htsfSampleService.findBySequencerRunId(192405L));
        // sampleList.addAll(htsfSampleService.findBySequencerRunId(191192L));

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                "biodev2.its.unc.edu"));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/nec.variantcalling");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            String format = "{\"entities\":[{\"entity_type\":\"Sample\",\"guid\":\"%d\"},{\"entity_type\":\"WorkflowRun\",\"name\":\"%s_L%d_%s_GATK\"}]}";
            for (Sample sample : sampleList) {

                if ("Undetermined".equals(sample.getBarcode())) {
                    continue;
                }

                Flowcell flowcell = sample.getFlowcell();
                String message = String.format(format, "rc_renci.svc", sample.getId(), flowcell.getName(),
                        sample.getLaneIndex(), sample.getName());
                System.out.println(message);
                producer.send(session.createTextMessage(message));
            }

        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }

}
