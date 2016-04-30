package com.athena.imis.models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Resource;

public class ExtendedCharacteristicSet implements Serializable {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6385631863370278023L;
	public transient Node subject;//, predicate;
	public transient Node predicate;
	public transient Node object;
	public transient HashMap<Integer, Integer> subjectBinds;
	public transient HashMap<Integer, Integer> objectBinds;
	public CharacteristicSet subjectCS ;
	public CharacteristicSet objectCS;
	Long longRep;
	public Long getLongRep() {
		return longRep;
	}

	public void setLongRep(Long longRep) {
		this.longRep = longRep;
	}

	//HashSet<Resource> properties;
	
	
	public ExtendedCharacteristicSet(Node subject, Resource predicate, Node object, HashSet<Resource> properties){
		
			this.subject = subject;
			//this.predicate = predicate;
			this.object = object;
			//this.properties = properties;
		
	}
	
	public ExtendedCharacteristicSet(Resource predicate, HashSet<Resource> properties, 
			CharacteristicSet subjectCS, CharacteristicSet objectCS){
				
		//this.predicate = predicate;
		//this.properties = properties;
		this.subjectCS = subjectCS;
		this.objectCS = objectCS;
		if(objectCS!=null){
			setLongRep(subjectCS.getLongRep()|objectCS.getLongRep());			
		}
		else
			setLongRep(subjectCS.getLongRep());
		
	
	}
	
	public ExtendedCharacteristicSet(CharacteristicSet subjectCS, CharacteristicSet objectCS) {		
		
		this.subjectCS = subjectCS;
		this.objectCS = objectCS;
		/*this.properties = new HashSet<Resource>();
		properties.addAll(subjectCS.properties);
		if(objectCS!=null){
			properties.addAll(objectCS.properties);
		}*/
		if(objectCS!=null){
			setLongRep(subjectCS.getLongRep()|objectCS.getLongRep());			
		}
		else
			setLongRep(subjectCS.getLongRep());
		
	}
	public void initBinds(){
		this.subjectBinds = new HashMap<Integer, Integer>();
		this.objectBinds = new HashMap<Integer, Integer>();
	}
	@Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(subjectCS).
            //append(predicate).
            append(objectCS).
        	/*append(getLongRep()).
        	append(predicate).*/
            toHashCode();
    }
	
	@Override
    public boolean equals(Object obj) {
       if (!(obj instanceof ExtendedCharacteristicSet))
            return false;
        if (obj == this)
            return true;

        ExtendedCharacteristicSet rhs = (ExtendedCharacteristicSet) obj;
        return new EqualsBuilder().
            // if deriving: appendSuper(super.equals(obj)).
            append(subjectCS, rhs.subjectCS).
            //append(predicate, rhs.predicate).
            append(objectCS, rhs.objectCS).
        	//append(getLongRep(), rhs.getLongRep()).
        	//append(predicate, rhs.predicate).
            isEquals();
    }
	
	public void print(){
		
		/*if(objectCS!=null){
			for(int i = 0; i < Long.numberOfLeadingZeros((long)subjectCS.getLongRep()); i++) {
			      System.out.print('0');
			}
			System.out.println(Long.toBinaryString((long)subjectCS.getLongRep()));
			for(int i = 0; i < Long.numberOfLeadingZeros((long)objectCS.getLongRep()); i++) {
			      System.out.print('0');
			}
			System.out.println(Long.toBinaryString((long)objectCS.getLongRep()));
			for(int i = 0; i < Long.numberOfLeadingZeros((long)getLongRep()); i++) {
			      System.out.print('0');
			}
			System.out.println(Long.toBinaryString((long)getLongRep()));
			System.out.println("--------------------------------");
		}
		else{
			for(int i = 0; i < Long.numberOfLeadingZeros((long)subjectCS.getLongRep()); i++) {
			      System.out.print('0');
			}
			System.out.println(Long.toBinaryString((long)subjectCS.getLongRep()));			
			for(int i = 0; i < Long.numberOfLeadingZeros((long)getLongRep()); i++) {
			      System.out.print('0');
			}
			System.out.println(Long.toBinaryString((long)getLongRep()));
			System.out.println("--------------------------------");
		}*/
		for(int i = 0; i < Long.numberOfLeadingZeros((long)getLongRep()); i++) {
		      System.out.print('0');
		}
		System.out.println(Long.toBinaryString((long)getLongRep()));//+" "+predicate.toString());
	}

}
