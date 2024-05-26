package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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

//        var mergedStream = getStringStringKStream(streamsBuilder);

        var mergedStream = getCustomStringStringKStream(streamsBuilder);

        mergedStream.print(Printed.<String, Greeting>toSysOut().withLabel("greetingsStream"));

        var modifiedStream = mergedStream.
        mapValues((readOnlyKey, value) -> new Greeting(value.getMessage(), value.getTimeStamp()));

        modifiedStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedStream"));

        modifiedStream.to(GREETINGS_UPPERCASE
                , Produced.with(Serdes.String(), SerdesFactory.grettingSerdes())
                );

        return streamsBuilder.build();
    }

    private static KStream<String, String> getStringStringKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> greetingsStream = streamsBuilder.stream(GREETINGS
                // , Consumed.with(Serdes.String(), Serdes.String())
                 );

        KStream<String, String> greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH
                // , Consumed.with(Serdes.String(), Serdes.String())
                 );


        return greetingsStream.merge(greetingsSpanishStream);
    }

    private static KStream<String, Greeting> getCustomStringStringKStream(StreamsBuilder streamsBuilder) {
        KStream<String, Greeting> greetingsStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), SerdesFactory.grettingSerdes())
                // , Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, Greeting> greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH, Consumed.with(Serdes.String(), SerdesFactory.grettingSerdes())
                // , Consumed.with(Serdes.String(), Serdes.String())
        );


        return greetingsStream.merge(greetingsSpanishStream);
    }


}
