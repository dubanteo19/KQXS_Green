package org.core;
import javax.mail.*;
import javax.mail.internet.*;
import java.util.Properties;
public class EmailService {

    public void sendEmail(String recipient, String subject, String body) {
        String host = "smtp.gmail.com";
        String port = "587"; // Cổng SMTP
        String senderEmail = "dhhceramic@gmail.com";
        String senderPassword = "xcjxqnyyecneuuxk";

        Properties properties = new Properties();
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.smtp.host", host);
        properties.put("mail.smtp.port", port);

        Session session = Session.getInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(senderEmail, senderPassword);
            }
        });

        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(senderEmail));
            message.setRecipients(
                    Message.RecipientType.TO, InternetAddress.parse(recipient));
            message.setSubject(subject);
            message.setText(body);

            Transport.send(message);
            System.out.println("Email sent successfully to " + recipient);
        } catch (MessagingException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to send email.", e);
        }
    }

}
