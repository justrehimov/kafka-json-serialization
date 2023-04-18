package com.example.kafkaconsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
@RequiredArgsConstructor
public class KafkaConsumerApplication {

    private final ObjectMapper objectMapper;

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }

    @SneakyThrows
    @KafkaListener(topics = "message", groupId = "grp1")
    public void consume(String json) {
        JsonNode jsonNode = objectMapper.readTree(json);
        Address address = new Address();
        address.setAddress(jsonNode.get("address").get("address").asText());
        address.setZipCode(jsonNode.get("address").get("zipCode").asText());

        Person person = new Person();
        person.setId(jsonNode.get("id").asLong());
        person.setName(jsonNode.get("name").asText());

        List<String> phones = new ArrayList<>();
        jsonNode.get("phones").elements().forEachRemaining(node -> phones.add(node.asText()));
        person.setPhones(phones);

        System.out.println(person);

        System.out.println("_____________________");
        System.out.println(objectMapper.readValue(json, Person.class));
    }


    @Data
    static class Person implements Serializable {
        private Long id;
        private String name;
        private Integer age;
        private BigDecimal salary;
        private LocalDate birthDate;
        private Address address;
        private List<String> phones;
    }

    @Data
    static class Address implements Serializable{
        private String zipCode;
        private String address;
    }

}
