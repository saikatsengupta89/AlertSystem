package alert.system
import java.util.Properties
import javax.mail.{Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}

import scala.io.Source


object sendEmail {
  
    val host = "smtp.gmail.com"
    val port = "587"
    
    //user whom you want to send the alert emails on missing data and exception data
    val address  = "saikat.sengupta89@gmail.com"
    
    //email configuration from where you want to send the alerts. Mostly organization SMTP general email address provided.
    val username = "saikat.sengupta89.learn@gmail.com"
    val password = "********"
  
    def sendMail(text:String, subject:String) = {
      val properties = new Properties()
      properties.put("mail.smtp.port", port)
      properties.put("mail.smtp.auth", "true")
      properties.put("mail.smtp.starttls.enable", "true")
  
      val session = Session.getDefaultInstance(properties, null)
      val message = new MimeMessage(session)
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(address));
      message.setSubject(subject)
      message.setContent(text, "text/html")
  
      val transport = session.getTransport("smtp")
      transport.connect(host, username, password)
      transport.sendMessage(message, message.getAllRecipients)
    }
  
}