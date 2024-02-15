//***************************************************************
//
//Developer:    Marshal Pfluger
//
//Project #:    Capstone
//
//File Name:    FlightWritable.java
//
//Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//Due Date:     12/08/2023
//
//Instructor:   Prof. Fred Kumi 
//
//Description:  This class contains the new datatype for the project
//
//***************************************************************

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class FlightWritable implements Writable {
	private int passengersCount;
	private int tripsCount;
	private String flightDate;
	private String flightNumber;

	//***************************************************************
  //
  //  Method:       FraudWritable constructor
  // 
  //  Description:  no arguments, calls 4 argument constructor 
  //
  //  Parameters:   N/A
  //
  //  Returns:      N/A 
  //
  //**************************************************************
	public FlightWritable() {
		this(0, 0, "", "");
	}
	
	//***************************************************************
	  //
	  //  Method:       FraudWritable constructor
	  // 
	  //  Description:  4 argument 
	  //
	  //  Parameters:   N/A
	  //
	  //  Returns:      N/A 
	  //
	  //**************************************************************
	public FlightWritable(int passengers, int trips, String date, String flightNumber) {
		setPassengers(passengers);
		setTripsCount(trips);
		setFlightDate(date);
		setFlightNumber(flightNumber);
	}

    // Add getters and setters
    public String getFlightNumber() {
        return flightNumber;
    }

    public void setFlightNumber(String flightNumber) {
        this.flightNumber = flightNumber;
    }

    public int getPassengers() {
        return passengersCount;
    }

    public void setPassengers(int paidPassengers) {
        this.passengersCount = paidPassengers;
    }

    public String getFlightDate() {
        return flightDate;
    }

    public void setFlightDate(String flightDate) {
        this.flightDate = flightDate;
    }
    
    public void setTripsCount(int trips) {
    	this.tripsCount = trips;
    }
    
    public int getTripsCount() {
    	return this.tripsCount;
    }
  
	//***************************************************************
  //
  //  Method:       write override
  // 
  //  Description:  overrides the write method to write out the datatype
  //
  //  Parameters:   N/A
  //
  //  Returns:      N/A 
  //
  //**************************************************************
  @Override
  public void write(DataOutput out) {
      // Write instance variables to the DataOutput
  	try {
			WritableUtils.writeVInt(out, this.passengersCount);
			WritableUtils.writeVInt(out, this.tripsCount);
	    	WritableUtils.writeString(out, this.flightDate);
	    	WritableUtils.writeString(out, this.flightNumber);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
			}
  	}
  
	//***************************************************************
  //
  //  Method:       readFields override
  // 
  //  Description:  overrides the readFields method to read the datatype
  //
  //  Parameters:   N/A
  //
  //  Returns:      N/A 
  //
  //**************************************************************
  @Override
  public void readFields(DataInput in) {
      // Read instance variables from the DataInput
  	try {
  		    this.passengersCount = WritableUtils.readVInt(in);
  		    this.tripsCount = WritableUtils.readVInt(in);
  		    this.flightDate = WritableUtils.readString(in);
  		    this.flightNumber = WritableUtils.readString(in);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
			}
  	}  
  }