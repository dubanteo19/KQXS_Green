package org.core;

import org.core.model.MailConfig;
import org.core.util.PropertiesHelper;

import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.*;

public class MailService {
    MailConfig mailConfig;


    public MailService(MailConfig mailConfig) {
        this.mailConfig = mailConfig;
    }

    public void sendEmail(String toEmail, String subject, String body) {
        // Set properties for the mail session
        if (!PropertiesHelper.enableMailService) {
            return;
        }
        Properties properties = new Properties();
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.smtp.host", this.mailConfig.getHost());
        properties.put("mail.smtp.port", this.mailConfig.getPort());

        // Create a Session with authentication
        Session session = Session.getInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(mailConfig.getUsername(), mailConfig.getPassword());
            }
        });


        try {
            // Create a MimeMessage object
            MimeMessage message = new MimeMessage(session);

            // Set From email address
            message.setFrom(new InternetAddress(mailConfig.getUsername()));

            // Set recipient email address
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmail));

            // Set the subject
            message.setSubject(subject);

            // Set the email body
            message.setText(body);

            // Send the email
            Transport.send(message);

            System.out.println("Email sent successfully.");
        } catch (MessagingException e) {
            e.printStackTrace();
            System.out.println("Error sending email.");
        }
    }
}
