package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;

public class GreetingTopology {

    private GreetingTopology() {
        throw new IllegalStateException("Utility Class");
    }

    public static final String GREETINGS = "greetings";
    public static final String  GREETINGS_UPPERCASE = "greetings_uppercase";
    public static final String  GREETINGS_SPANISH = "greetings_spanish";

    public static Topology builTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var greetingsStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));

        var greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH, Consumed.with(Serdes.String(), Serdes.String()));

        var mergedStream = greetingsStream.merge(greetingsSpanishStream);

        mergedStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));

        var modifiedStream = mergedStream.
//                .flatMapValues((readOnlykey, value) -> Arrays.asList(value.split("")));
        mapValues((readOnlyKey, value) -> value.toUpperCase());

        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));

        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }

}
