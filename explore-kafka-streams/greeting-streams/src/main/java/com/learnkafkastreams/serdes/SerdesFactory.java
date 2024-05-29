package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    static public Serde<Greeting> grettingSerdes() {
        System.out.println("In here - factory");
        return new GreetingSerdes();
    }


    static public Serde<Greeting> grettingSerdesUsingGenerics() {
        JsonSerializer jsonSerializer = new JsonSerializer();
        JsonDeserializer jsonDeserializer = new JsonDeserializer(Greeting.class);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

}
