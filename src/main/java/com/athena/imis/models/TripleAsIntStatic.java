package com.athena.imis.models;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;

public class TripleAsIntStatic implements Serializable {
		
		/**
	 * 
	 */
	private static final long serialVersionUID = 6592083097027722200L;
		public static final SimpleAttribute<TripleAsIntStatic, Integer> S = new SimpleAttribute<TripleAsIntStatic, Integer>("carId") {
	        public Integer getValue(TripleAsIntStatic tai, QueryOptions queryOptions) { return tai.s; }
	    };
	    public static final SimpleAttribute<TripleAsIntStatic, Integer> O = new SimpleAttribute<TripleAsIntStatic, Integer>("carId") {
	        public Integer getValue(TripleAsIntStatic tai, QueryOptions queryOptions) { return tai.o; }
	    };
	    public int s;
		public int p;
		public int o;
		
		public TripleAsIntStatic(int s, int p){
			this.s = s ;
			this.p = p ;
			//this.o = o ;
		}
		
		public TripleAsIntStatic(int s, int p, int o){
			this.s = s ;
			this.p = p ;
			this.o = o ;
		}
		
		public TripleAsIntStatic(int s){
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
	       if (!(obj instanceof TripleAsIntStatic))
	            return false;
	        if (obj == this)
	            return true;

	        TripleAsIntStatic rhs = (TripleAsIntStatic) obj;
	        return new EqualsBuilder().
	            // if deriving: appendSuper(super.equals(obj)).
	            append(s, rhs.s).
	            append(p, rhs.p).
	            append(o, rhs.o).
	            isEquals();
	    }
			
}
