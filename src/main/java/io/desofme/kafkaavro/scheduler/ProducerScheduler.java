package io.desofme.kafkaavro.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Configuration
@RequiredArgsConstructor
public class ProducerScheduler {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;


    @Scheduled(fixedDelay = 8000)
    public void writeData() throws JsonProcessingException {

        Address address = new Address();
        address.setAddress(UUID.randomUUID().toString().substring(4, 12));
        address.setZipCode(UUID.randomUUID().toString());

        Person person = new Person();
        person.setId((long) new Random().nextInt(200));
        person.setAge(21);
        person.setName("Vusal");
        person.setBirthDate(LocalDate.now());
        person.setPhones(List.of("9945762376", "99102837875", "9947776126"));
        person.setSalary(BigDecimal.valueOf(new Random().nextDouble()));
        person.setAddress(address);


        var json = objectMapper.writeValueAsString(person);
        kafkaTemplate.send("message", json);
    }

    @Data
    class Person implements Serializable {
        private Long id;
        private String name;
        private Integer age;
        private BigDecimal salary;
        private LocalDate birthDate;
        private Address address;
        private List<String> phones;
    }

    @Data
    class Address implements Serializable{
        private String zipCode;
        private String address;
    }

}
