package org.example.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class BlogReceiver {
    private static final String EXCHANGE_NAME = "BlogApp";
    private static final String DELIMITER = " ";

    private volatile boolean menu = true;

    private final Object mon = new Object();

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        Scanner in = new Scanner(System.in);

        String queueName = channel.queueDeclare().getQueue();
        System.out.println("My queue name: " + queueName);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
//            System.out.println(Thread.currentThread().getName());
        };

        BlogReceiver blogReceiver = new BlogReceiver();

        Thread thread1 = new Thread(() -> {
            blogReceiver.doMenu(channel,queueName,in);
        });

        Thread thread2 = new Thread(() -> {
            blogReceiver.readBlog(channel,queueName,deliverCallback);
        });

        thread1.start();
        thread2.start();


    }

    public void doMenu(Channel channel, String queueName, Scanner in) {
        synchronized (mon) {
            try {
                while(true) {
                    if (!menu) {
                        mon.wait();
                    }
                    setTopic(channel, queueName, in);
                    menu = false;
                    mon.notify();
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void readBlog(Channel channel, String queueName, DeliverCallback deliverCallback) {
        synchronized (mon) {
            try {
                while (true) {
                    if (menu) {
                        mon.wait();
                    }
                    System.out.println(" [*] Waiting for messages");
                    channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                    });
                    Thread.sleep(15000);
                    menu = true;
                    mon.notify();
                }


            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private static void setTopic(Channel channel, String queueName, Scanner in) throws IOException {
        String command = "";
        String[] result;
        String header = "";
        String body = "";
        System.out.print("Type command ('set_topic <topic>' - to set topic, 'unsubscribe <topic>' to unsubscribe from topic): ");
        command = in.nextLine();
        result = command.split(BlogReceiver.DELIMITER, 2);
        if (result.length > 1) {
            header = result[0];
            body = result[1];
            if (header.equals("set_topic")) {
                channel.queueBind(queueName, EXCHANGE_NAME, body);
            } else if (header.equals("unsubscribe")) {
                channel.queueUnbind(queueName, EXCHANGE_NAME, body);
            }
        }
    }
}