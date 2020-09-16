import com.itextpdf.forms.PdfAcroForm;
import com.itextpdf.forms.fields.PdfFormField;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfReader;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class DeductionConsumer {
    private final static String EXCHANGE_NAME = "documents";
    private final static String EXCHANGE_TYPE = "fanout";
    private final static String DEST_FOLDER = "files/deduction/";
    private final static String SRC = "files/deduction.pdf";

    /*
        info[0] - name
        info[1] - surname
        info[2] + info[3] - passport
        info[4] - age
        info[5] - date
     */
    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        try {
            Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            channel.basicQos(3);

            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
            // создаем временную очередь со случайным названием
            String queue = channel.queueDeclare().getQueue();

            // привязали очередь к EXCHANGE_NAME
            channel.queueBind(queue, EXCHANGE_NAME, "");

            DeliverCallback deliverCallback = (consumerTag, message) -> {
                System.out.println("Creating deduction file for " + new String(message.getBody()));
                String[] info = new String(message.getBody()).split(" ");
                try {
                    String fileName = DEST_FOLDER + info[0] + " " + info[1] + ".pdf";
                    File file = new File(fileName);
                    file.getParentFile().getParentFile().mkdirs();

                    PdfReader pdfReader = new PdfReader(SRC);
                    PdfWriter pdfWriter = new PdfWriter(fileName);
                    PdfDocument doc = new PdfDocument(pdfReader, pdfWriter);
                    PdfAcroForm form = PdfAcroForm.getAcroForm(doc, true);
                    Map<String, PdfFormField> fields = form.getFormFields();
                    fields.get("name").setValue(info[0]);
                    fields.get("surname").setValue(info[1]);
                    fields.get("age").setValue(info[4]);
                    fields.get("passport").setValue(info[2] + " " + info[3]);
                    fields.get("given").setValue(info[5]);

                    doc.close();
                    channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                } catch (IOException e) {
                    System.err.println("FAILED");
                    channel.basicReject(message.getEnvelope().getDeliveryTag(), false);
                }
            };

            channel.basicConsume(queue, false, deliverCallback, consumerTag -> {
            });
        } catch (IOException | TimeoutException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
