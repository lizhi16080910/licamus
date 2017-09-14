package com;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;

/**
 * Created by lfq on 2016/10/18.
 */
public class SendMail {
    public static void sendMail() throws Exception {
        Properties props = new Properties(); //可以加载一个配置文件
        props.put("mail.smtp.host", "mail.fastweb.com.cn");//存储发送邮件服务器的信息
        props.put("mail.smtp.auth", "true"); //同时通过验证
        Session session = Session.getInstance(props); //根据属性新建一个邮件会话
        //        session.setDebug(true); //有他会打印一些调试信息。

        MimeMessage message = new MimeMessage(session);// 由邮件会话新建一个消息对象
        message.setFrom(new InternetAddress("lifq@fastweb.com.cn")); //设置发件人的地址
        message.setRecipient(Message.RecipientType.TO, new InternetAddress("lifq@fastweb.com.cn")); //设置收件人,并设置其接收类型为TO
        message.setSubject("多玩日志合并监控"); //设置标题

        //设置信件内容
        message.setText("test"); //发送 纯文本 邮件 todo
        //message.setContent("<a>html 元素</a>：<b>邮件内容</b>", "text/html;charset=gbk"); //发送HTML邮件，内容样式比较丰富
        message.setSentDate(new Date());//设置发信时间
        message.saveChanges();//存储邮件信息

        //发送邮件
//        Transport transport = session.getTransport("smtp");
        Transport transport = session.getTransport("smtp");
        transport.connect("lifq@fastweb.com.cn", "fastweb.com.cn123");
        transport.sendMessage(message, message.getAllRecipients());//发送邮件,其中第二个参数是所有已设好的收件人地址
        transport.close();
    }

    public static void main(String[] args) throws Exception {
        sendMail();
    }

}
