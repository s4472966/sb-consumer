package org.zzz.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import mil.nga.tiff.TIFFImage;
import mil.nga.tiff.TiffWriter;
import mil.nga.tiff.util.TiffConstants;
import mil.nga.tiff.FileDirectory;
import mil.nga.tiff.Rasters;
import redis.clients.jedis.Jedis;
import ij.ImageJ;





public class MyConsumer {
	private final static String TOPIC = "my-test-1";
	private final static String BOOTSTRAP_SERVERS = "localhost:192.168.1.130:9092";
	
	
	private static Consumer<Long, String> createConsumer() {
	      final Properties props = new Properties();
	      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
	                                  BOOTSTRAP_SERVERS);
	      props.put(ConsumerConfig.GROUP_ID_CONFIG,
	                                  "MyConsumer");
	      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
	              LongDeserializer.class.getName());
	      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
	              StringDeserializer.class.getName());
	      // Create the consumer using props.
	      final Consumer<Long, String> consumer =
	                                  new KafkaConsumer<>(props);
	      // Subscribe to the topic.
	      consumer.subscribe(Collections.singletonList(TOPIC));
	      return consumer;
	}
	
	static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        final int giveUp = 100;   int noRecordsCount = 0;
        
        
        
        @SuppressWarnings("resource")
		Jedis client = new Jedis("192.168.1.130:6379");
		
        
        while (true) {
        	
            @SuppressWarnings("deprecation")
			final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                //record.value() is the uuid code from the producer
            	String data = client.get(record.value());
            	String[] info = null;
            	int xDim = 0, yDim = 0, zDim, num_channels = 0;
            	if (data.contains("info")) {
            		info = data.split(";");
            		xDim = Integer.parseInt(info[1]);
            		yDim = Integer.parseInt(info[2]);
            		zDim = Integer.parseInt(info[3]);
            		num_channels = Integer.parseInt(info[4]);
            	} else {
            		try {
            			File file = new File("E:/UQ/Thesis/test_files/tiff_" + System.currentTimeMillis() + ".tiff");
						file.createNewFile();
						TIFFImage tiff = new TIFFImage();
						tiff = tiff_write(xDim, yDim, num_channels, Integer.parseInt(data));
						TiffWriter.writeTiff(file, tiff);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
            	
            	
            	
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
	
	public static TIFFImage tiff_write(int w, int h, int num_channels, int data) {
		int[] dataArray = Int_to_array(data);
		int index = 0;
		Rasters rasters = new Rasters(w, h, num_channels, 16, 1);
		int rowsPerStrip = rasters.calculateRowsPerStrip(TiffConstants.PLANAR_CONFIGURATION_CHUNKY);
		
		FileDirectory directory = new FileDirectory();
		directory.setImageWidth(w);
		directory.setImageHeight(h);
		directory.setCompression(TiffConstants.COMPRESSION_NO);
		directory.setSamplesPerPixel(num_channels);
		directory.setRowsPerStrip(rowsPerStrip);
		directory.setResolutionUnit(1);
		directory.setPlanarConfiguration(TiffConstants.PLANAR_CONFIGURATION_CHUNKY);
		for (int i = 0; i < num_channels; ++i) {
			directory.setBitsPerSample(16);
			directory.setSampleFormat(TiffConstants.SAMPLE_FORMAT_UNSIGNED_INT);
		}
		directory.setPhotometricInterpretation(TiffConstants.PHOTOMETRIC_INTERPRETATION_RGB);
		for(int y = 0; y < h; y++ ) {
			for(int x = 0; x < w; x++) {
				float pixelValue = dataArray[index++];
				rasters.setFirstPixelSample(x, y, pixelValue);
			}
		}
		
		TIFFImage tiff = new TIFFImage();
		tiff.add(directory);
		
		return tiff;
		
	}
	
	static int[] Int_to_array(int n) {

		int j = 0;

		int len = Integer.toString(n).length();

		int[] arr = new int[len];

		while(n!=0) {

			arr[len-j-1]=n%10;

			n=n/10;

			j++;

		}

		return arr;

	}
	
	
	public static void main(String... args) throws Exception {
		runConsumer();
	}
}
