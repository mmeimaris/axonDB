package com.athena.imis.models;

import java.util.HashMap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class BigECSTuple {

	public int property ;
	
	public BigExtendedCharacteristicSet ecs ;
	
	public TripleAsInt triplePattern ;
	
	public HashMap<Integer, Integer> subjectBinds;
	
	public HashMap<Integer, Integer> objectBinds;
	
	public int card ;
	
	public BigECSTuple(BigExtendedCharacteristicSet ecs, int property, TripleAsInt triplePattern){
		
		this.property = property;
		
		this.ecs = ecs;
		
		this.triplePattern = triplePattern;
	}
	
	@Override
    public int hashCode() {		
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers            
            append(ecs).        	
            toHashCode();
    }
	
	@Override
    public boolean equals(Object obj) {
       if (!(obj instanceof BigECSTuple))
            return false;
        if (obj == this)
            return true;

        BigECSTuple rhs = (BigECSTuple) obj;
        return new EqualsBuilder().            
        	append(ecs, rhs.ecs).
            isEquals();
    }
	
	@Override
	public String toString(){
		return ecs.toString();
	}
	
}
