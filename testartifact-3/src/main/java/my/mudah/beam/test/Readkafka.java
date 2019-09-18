package my.mudah.beam.test;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;

import com.google.cloud.spanner.Options;
import com.google.common.collect.ImmutableMap;
import com.sun.jndi.url.iiopname.iiopnameURLContextFactory;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.io.File;
import java.io.IOException;

import my.mudah.beam.test.action_states_pkey;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Readkafka {
	private static final Logger LOG = LoggerFactory.getLogger(Readkafka.class);
	 

	public static void main(String[] args) throws IOException {
		 io.confluent.kafka.serializers.KafkaAvroDeserializer MyKafkaAvroDeserializer;
		 class ConvertAvroToCsv extends DoFn<GenericRecord, String> {
				
			    private String delimiter;
			    private String schemaJson;
		
			    public ConvertAvroToCsv(String schemaJson, String delimiter) {
			      this.schemaJson = schemaJson;
			      this.delimiter = delimiter;
			    };
		 };
        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create());

        
//        p.apply(KafkaIO.<String, String>read()
//                .withBootstrapServers("10.0.0.222:9092")
//                .withTopic("m56.action_states")
//                .withKeyDeserializer(StringDeserializer.class)
//                .withValueDeserializer(StringDeserializer.class)
//
//                // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
//                // the first 5 records.
//                // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
//                .withMaxNumRecords(5)
//
//                .withoutMetadata())
//		        .apply(
//		              ParDo.of(
//	            		  new DoFn<KafkaRecord<String, String>, String>() {
//		                     @ProcessElement
//		                     public void processElement(ProcessContext processContext) {
//		                       KafkaRecord<String, String> record = processContext.element();
//		//                           processContext.output(record.getKV().getValue());
//		                       LOG.info("HERE IS MY PRINT".concat(record.getKV().getValue()));
//		                     }
//		                   }));
        		
        
//        PCollection<KafkaRecord<String, Object>> kafkaInput =
//        p.apply(
//                KafkaIO.<String, Object>read()
//                    .withBootstrapServers("10.0.0.222:9092")
//                    .withTopic("m56.action_states")
//                    .withKeyDeserializer(StringDeserializer.class)
//                    .withValueDeserializer(KafkaAvroDeserializer.class)
//                    .withMaxNumRecords(5)
//                    .updateConsumerProperties(ImmutableMap.of("schema.registry.url", (Object)"http://10.0.0.35:32100/")));
//        			
//			        
//             
//        			
//        			
//        
//       .apply(
//           ParDo.of(
//               new DoFn<KafkaRecord<String, String>, String>() {
//                  @ProcessElement
//                  public void processElement(ProcessContext processContext) {
//                    KafkaRecord<String, String> record = processContext.element();
////                        processContext.output(record.getKV().getValue());
//                    LOG.info("HERE IS MY PRINT".concat(record.getKV().getValue()));
//                  }
//                }));
//        p.apply(
//                KafkaIO.<String, String>read()
//                    .withBootstrapServers("10.0.0.222:9092")
//                    .withTopic("m56.action_states")
//                    .withKeyDeserializer(StringDeserializer.class)
//                    .withValueDeserializer(StringDeserializer.class)
//                    .withMaxNumRecords(5)
//                    .updateConsumerProperties(ImmutableMap.of("schema.registry.url", (Object)"http://10.0.0.35:32100")));
//            .apply(
//                ParDo.of(
//                    new DoFn<KafkaRecord<String, String>, String>() {
//                      @ProcessElement
//                      public void processElement(ProcessContext processContext) {
//                        KafkaRecord<String, String> record = processContext.element();
//                        processContext.output((String) record.getKV().getValue());
//                      }
//                    }));
        Schema schema = new Schema.Parser().parse(new File("/Users/nicholas/Desktop/Deskstop/action_states_pkey.avsc"));
        PTransform<PBegin, PCollection<KV<action_states_pkey, String>>> kafka =
                KafkaIO.<action_states_pkey, String>read()
                    .withBootstrapServers("10.0.0.222:9092")
                    .withTopic("m56.ad_actions")
//                    .withKeyDeserializer(io.confluent.kafka.serializers.KafkaAvroDeserializer.class)
                    .withKeyDeserializerAndCoder((Class)KafkaAvroDeserializer.class, AvroCoder.of(action_states_pkey.class))
                    .withValueDeserializer(StringDeserializer.class)
                    .updateConsumerProperties(ImmutableMap.of("schema.registry.url", (Object)"http://10.0.0.35:32100"))
                    .updateConsumerProperties(ImmutableMap.of("specific.avro.reader", (Object)"true"))
                    .withMaxNumRecords(5)
                    .withoutMetadata();

        
        p.apply(kafka)
        	.apply(Keys.<action_states_pkey>create())
//        	.apply(TextIO.write().to("gs://sg-dataflow").withSuffix(".csv"))
            .apply("ExtractWords", ParDo.of(new DoFn<action_states_pkey, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                	action_states_pkey key = c.element();
                    c.output(key.getAdId().toString());
                }
            }));
        
//        	.apply("ExtractWords", ParDo.of(new DoFn<Object, String>() {
//            @ProcessElement
//            public void processElement(ProcessContext c) {
//            	Object key = c.element();
//                c.output(key.toString());;
//            }
//        }));
        
//        p.apply(KafkaIO.<byte[], byte[]>read()
//                .withBootstrapServers("10.0.0.222:9092")
//                .withTopic("m56.action_states")
//                .withKeyDeserializer(ByteArrayDeserializer.class)
//                .withValueDeserializer(ByteArrayDeserializer.class)
//                .withoutMetadata())
//                .apply(Keys.<byte[]>create())
//                .apply("ParseAvro", ParDo.of(new DoFn<byte[], action_states_pkey>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                    	  action_states_pkey data = (action_states_pkey)
//                    	avroDecoder.fromBytes(c.element());
//                        c.output(data);
//                    }
//                }));
        
        p.run().waitUntilFinish();
    }
};


