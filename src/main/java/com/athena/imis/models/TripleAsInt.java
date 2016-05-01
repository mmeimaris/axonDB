package com.athena.imis.models;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class TripleAsInt implements Serializable {
		
		/**
	 * 
	 */
	private static final long serialVersionUID = 6592083097027722200L;
		public int s;
		public int p;
		public int o;
		
		public TripleAsInt(int s, int p){
			this.s = s ;
			this.p = p ;
			//this.o = o ;
		}
		
		public TripleAsInt(int s, int p, int o){
			this.s = s ;
			this.p = p ;
			this.o = o ;
		}
		
		public TripleAsInt(int s){
			this.s = s ;					
		}
		
		
		@Override
	    public int hashCode() {
	        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
	            // if deriving: appendSuper(super.hashCode()).
	            append(s).
	            append(p).
	            append(o).
	            toHashCode();
	    }
		
		@Override
	    public boolean equals(Object obj) {
	       if (!(obj instanceof TripleAsInt))
	            return false;
	        if (obj == this)
	            return true;

	        TripleAsInt rhs = (TripleAsInt) obj;
	        return new EqualsBuilder().
	            // if deriving: appendSuper(super.equals(obj)).
	            append(s, rhs.s).
	            append(p, rhs.p).
	            append(o, rhs.o).
	            isEquals();
	    }
			
}
