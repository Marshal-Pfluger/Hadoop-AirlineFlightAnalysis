//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Capstone
//
//  File Name:    CapstoneMapReduce.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     12/08/2023
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  This is a data analysis for flight data
//
//***************************************************************

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CapstoneMapper extends Mapper<LongWritable, Text, Text, FlightWritable> {

	//***************************************************************
    //
    //  Method:       map
    // 
    //  Description:  map override 
    //
    //  Parameters:   LongWritable key, Text value, Context context
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    @Override
    public void map(LongWritable key, Text value, Context context) {
        try {
            // Split the input line into columns using a comma as the delimiter
            String[] line = value.toString().split(",");

            // Check if the input line has all 6 columns
            if (line.length == 6) {
            	// Split the date to extract the month
                String[] flightDate = line[5].split("-");
                // Stor the month
                String month = flightDate[1];
                // Store Flightnumber
                String flightNumber = line[0];
                // Parse and store the number of passengers
                int passengersCount = Integer.parseInt(line[4]);
                // set trip count to one to get num of trips in reducer
                int tripCount = 1;
            	// Write out Key value pair
                context.write(new Text(flightNumber + "," + month), new FlightWritable (passengersCount, tripCount, line[5], flightNumber));
                }
            
        } catch (Exception e) {
            // Handle any exceptions that occur during processing
            e.printStackTrace();
            System.exit(0);
        }
    }
}