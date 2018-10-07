package stubs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NameWritable implements  WritableComparable<NameWritable> {

	String firstName, lastName;
	
	public NameWritable(){}
	
	public NameWritable(String firstName,String lastName){
		this.firstName = firstName;
		this.lastName = lastName;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.firstName = in.readUTF();
		this.lastName = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.firstName);
		out.writeUTF(this.lastName);
		
	}

	@Override
	public int compareTo(NameWritable other) {
		//
		String fullName = this.firstName + this.lastName;
		String fullName_other=other.firstName + this.lastName;
		return (fullName.compareTo(fullName_other));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((firstName == null) ? 0 : firstName.hashCode());
		result = prime * result
				+ ((lastName == null) ? 0 : lastName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NameWritable other = (NameWritable) obj;
		if (firstName == null) {
			if (other.firstName != null)
				return false;
		} else if (!firstName.equals(other.firstName))
			return false;
		if (lastName == null) {
			if (other.lastName != null)
				return false;
		} else if (!lastName.equals(other.lastName))
			return false;
		return true;
	}

	
}
