//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Capstone
//
//  File Name:    CapstoneReducer.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     10/08/2023
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  This class contains the reducer for the hadoop project
//
//***************************************************************

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;



public class CapstoneReducer extends Reducer<Text, FlightWritable, Text, Text> {
	private ArrayList<FlightWritable> monthsList = new ArrayList<>();;
	private ArrayList<FlightWritable> underUtilizedList = new ArrayList<>();;
    
	
	//***************************************************************
    //
    //  Method:       reduce
    // 
    //  Description:  reduce override 
    //
    //  Parameters:   Text key, Iterable<FlightWritable> values, Context context
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    public void reduce(Text key, Iterable<FlightWritable> values, Context context) {
    	
        // Declare variable to hold totals
        int totalPassengers = 0;
        int totalTrips = 0;
        String flightNumber = null;
        
        // Split the Key to get access the month
        String[] keyCheck = key.toString().split(",");
        
        // Iterate through values to sum the passengers and trip counts
        for (FlightWritable val : values) {
            totalPassengers += val.getPassengers();
            totalTrips += val.getTripsCount();
            flightNumber = val.getFlightNumber();
        }
        
        // Add the month to the list of month once reduced
        monthsList.add(new FlightWritable(totalPassengers, totalTrips, keyCheck[1], keyCheck[0]));
        

        // If the month counts as underutilized add it to the list
        if (totalTrips > 25 && totalPassengers < 3000) {
        	System.out.print(totalTrips + ":" + totalPassengers);
        	underUtilizedList.add(new FlightWritable(totalPassengers, totalTrips, "date", flightNumber));
        	}
    }

	//***************************************************************
    //
    //  Method:       cleanup
    // 
    //  Description:  cleanup override 
    //
    //  Parameters:   Context context
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    @Override
    protected void cleanup(Context context) {
    	try {
    		// Use a comparator to sort the list of monthly averages
        	monthsList.sort(Comparator.comparingDouble(obj -> {
        	    return ((FlightWritable) obj).getPassengers() / ((FlightWritable) obj).getTripsCount();
        	}).reversed());
        	
        	// Use streams to build a map of the flights for each month
            Map<String, List<FlightWritable>> groupedByMonth = monthsList.stream()
                    .collect(Collectors.groupingBy(FlightWritable::getFlightDate));
            context.write(new Text("Top Fligts Per Month:"), new Text());
            // Iterate through entries grouped by month
            groupedByMonth.forEach((month, monthEntries) -> {
                try {
                    // Display the current month
                    context.write(new Text("\n" + month), new Text());
                } catch (Exception e) {
                    // Handle any exceptions that occur during processing
                    e.printStackTrace();
                    System.exit(0);
                }

                // Process each entry within the current month, limit to 3 entries
                monthEntries.stream()
                        .limit(3)
                        .forEach(entry -> {
                            try {
                                // Write flight details with average passengers per trip
                                context.write(new Text(entry.getFlightNumber()), new Text(String.format("AVG: %.2f", (double) entry.getPassengers() / entry.getTripsCount())));
                            } catch (Exception e) {
                                // Handle any exceptions that occur during processing
                                e.printStackTrace();
                                System.exit(0);
                            }
                        });
            });
            
            // Output Underutilized Flights
            context.write(new Text("\nUnderutilized Flights:\n"), new Text());
            context.write(new Text("Flight"), new Text(String.format("%-12s%s", "Passengers", "NumFlights")));
            for (FlightWritable flight : underUtilizedList) {
                context.write(new Text(flight.getFlightNumber()), new Text(String.format("%-12d%d", flight.getPassengers() , flight.getTripsCount())));
            }
    	} catch(Exception e) {
            e.printStackTrace();
            System.exit(0);
    	}
    }
}