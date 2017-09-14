package com.fastweb.cdnlog.bigdata.util;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * Created by lfq on 2016/12/16.
 */
public class SendMailUtil {
    /**
     * @Title: main
     * @param args
     * @throws FileNotFoundException
     * @throws
     * @Description: TODO(这里用一句话描述这个方法的作用)
     */

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("SendMailUtil <conf file> <mail content> <Attachment file>");
            System.exit(1);
        }
        String confFile = args[0];
        Properties props = new Properties();
        InputStream is = new FileInputStream(confFile);
        props.load(is);
        is.close();

        String fromMail = props.getProperty("fromMail");
        String user = props.getProperty("user");
        String pwd = props.getProperty("pwd");
        String toMail = props.getProperty("toMail");
        String title = props.getProperty("title");
        StringBuilder mailContent = new StringBuilder(args[1]);
        String smtpHost = props.getProperty("smtpHost");
        File attachmentFile = null;
        if (args.length == 3) {
            attachmentFile = new File(args[2]);
        }
        sendMail(fromMail, user, pwd, toMail, title, mailContent.toString(), smtpHost,
                attachmentFile);
    }

    public static void sendMail(String fromMail, String user, String password, String toMail,
                                String mailTitle, String mailContent, String smtpHost, File attachment)
            throws Exception {
        Properties props = new Properties(); // 可以加载一个配置文件
        props.put("mail.smtp.host", smtpHost);// 存储发送邮件服务器的信息
        props.put("mail.smtp.auth", "true"); // 同时通过验证
        Session session = Session.getInstance(props); // 根据属性新建一个邮件会话
        // session.setDebug(true); //有他会打印一些调试信息。

        MimeMessage message = new MimeMessage(session);// 由邮件会话新建一个消息对象
        message.setFrom(new InternetAddress(fromMail)); // 设置发件人的地址
        message.setRecipient(Message.RecipientType.TO, new InternetAddress(toMail)); // 设置收件人,并设置其接收类型为TO
        message.setSubject(mailTitle); // 设置标题

        // 向multipart对象中添加邮件的各个部分内容，包括文本内容和附件
        Multipart multipart = new MimeMultipart();

        // 添加邮件正文
        BodyPart contentPart = new MimeBodyPart();
        contentPart.setText(mailContent);
        multipart.addBodyPart(contentPart);

        // 添加附件的内容
        if (attachment != null) {
            BodyPart attachmentBodyPart = new MimeBodyPart();
            DataSource source = new FileDataSource(attachment);
            attachmentBodyPart.setDataHandler(new DataHandler(source));

            // 网上流传的解决文件名乱码的方法，其实用MimeUtility.encodeWord就可以很方便的搞定
            // 这里很重要，通过下面的Base64编码的转换可以保证你的中文附件标题名在发送时不会变成乱码
            // sun.misc.BASE64Encoder enc = new sun.misc.BASE64Encoder();
            // messageBodyPart.setFileName("=?GBK?B?" +
            // enc.encode(attachment.getName().getBytes()) + "?=");

            // MimeUtility.encodeWord可以避免文件名乱码
            attachmentBodyPart.setFileName(MimeUtility.encodeWord(attachment.getName()));
            multipart.addBodyPart(attachmentBodyPart);
        }

        // 将multipart对象放到message中
        message.setContent(multipart);
        message.setSentDate(new Date());// 设置发信时间
        message.saveChanges();// 存储邮件信息

        // 发送邮件
        // Transport transport = session.getTransport("smtp");
        Transport transport = session.getTransport("smtp");
        transport.connect(user, password);
        transport.sendMessage(message, message.getAllRecipients());// 发送邮件,其中第二个参数是所有已设好的收件人地址
        transport.close();
    }
}
