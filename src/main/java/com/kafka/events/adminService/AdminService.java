package com.kafka.events.adminService;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;

@Service
public class AdminService {

        private final AdminClient adminClient;

        public AdminService(){
            this.adminClient = createAdmin();
        }

        private AdminClient createAdmin(){
            Properties props = new Properties();
            props.setProperty(
                    "bootstrap.servers",
                    "localhost:9092"
            );
            return AdminClient.create(props);
        }

        public void createTopic(String topicName){
            NewTopic topic = new NewTopic(
                    topicName,
                    3,
                    (short) 1
            );

            adminClient.createTopics(Collections.singleton(topic));
        }

        public void shutdown(){
            adminClient.close();
        }

}
