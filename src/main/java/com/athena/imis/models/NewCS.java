package com.athena.imis.models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class NewCS {
	
	List<Integer> asList ;
	
	Set<String> matches ;

	public List<Integer> getAsList() {
		return asList;
	}

	public void setAsList(List<Integer> asList) {
		this.asList = asList;
	}

	public Set<String> getMatches() {
		return matches;
	}

	public void setMatches(Set<String> matches) {
		this.matches = matches;
	}
	
	public NewCS(List<Integer> asList){
		this.asList = asList;
		Collections.sort(this.asList);
	}
	
	public NewCS(Integer[] asArray){
		
		List<Integer> intList = new ArrayList<Integer>();
		for (int index = 0; index < asArray.length; index++)
		{
		    intList.add(asArray[index]);
		}
		this.asList = intList; 
		Collections.sort(this.asList);
	}
	
	
	
	@Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(asList).           
            toHashCode();
    }
	
	@Override
    public boolean equals(Object obj) {
       if (!(obj instanceof NewCS))
            return false;
        if (obj == this)
            return true;

        NewCS rhs = (NewCS) obj;
        return new EqualsBuilder().
            // if deriving: appendSuper(super.equals(obj)).
            append(asList, rhs.asList).            
            isEquals();
    }
	
	@Override
	public String toString(){
		return asList.toString();
	}

}
