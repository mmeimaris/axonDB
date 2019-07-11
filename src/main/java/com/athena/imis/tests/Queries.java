package com.athena.imis.tests;

import java.util.ArrayList;
import java.util.List;

public class Queries {

	public static List<String> queries = new ArrayList<String>();
	
	public static List<String> reactome_queries = new ArrayList<String>();
	
	public static List<String> geonames_queries = new ArrayList<String>();
	
	public static List<String> sample_queries = new ArrayList<String>();
	
	{
		//queries.add(q1_ext);
				queries.add(q2);
				queries.add(q3_ext);
				queries.add(q4_ext);
				//queries.add(q5_ext);
				//queries.add(q8);
				queries.add(q8_ext);
				//queries.add(q9);
				queries.add(q10_ext);
				queries.add(q11_ext);
				queries.add(q12);
				queries.add(q13_ext);
				queries.add(qm1);
				queries.add(qm2);
				queries.add(qm3);
				queries.add(qm4);
				//queries.add(qm4_ext);
		
		reactome_queries.add(reactomePrefixes + " " + r1);
		reactome_queries.add(reactomePrefixes + " " + r2);
		reactome_queries.add(reactomePrefixes + " " + r3);
		reactome_queries.add(reactomePrefixes + " " + r4);
		reactome_queries.add(reactomePrefixes + " " + r5);
		reactome_queries.add(reactomePrefixes + " " + r6);
		reactome_queries.add(reactomePrefixes + " " + r7);
		reactome_queries.add(reactomePrefixes + " " + r8);
		reactome_queries.add(reactomePrefixes + " " + r9);
		reactome_queries.add(reactomePrefixes + " " + r10);
		reactome_queries.add(reactomePrefixes + " " + r11);
		reactome_queries.add(reactomePrefixes + " " + r12);
		
		geonames_queries.add(geonamesPrefixes + " " + g1);
		geonames_queries.add(geonamesPrefixes + " " + g2);
		geonames_queries.add(geonamesPrefixes + " " + g3);
		geonames_queries.add(geonamesPrefixes + " " + g4);
		geonames_queries.add(geonamesPrefixes + " " + g5);
		geonames_queries.add(geonamesPrefixes + " " + g6);
		
		sample_queries.add(prefix + " SELECT ?x ?y WHERE {?x rdf:type ub:UndergraduateStudent . ?x rdf:friendOf ?y. ?y rdf:type ub:UndergraduateStudent .}");		
	}
	public List<String> getQueries(int dataset){
		if(dataset == 1)
			return Queries.reactome_queries;
		else if(dataset == 2)
			return Queries.geonames_queries;
		else if(dataset == 4){
			return Queries.sample_queries;
		}
		return Queries.queries;
	}
	public static String prefix = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
			+ "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> ";
	
	/*public static String q1 = prefix + 
			"SELECT ?X WHERE"
			+ "{?X rdf:type ub:GraduateStudent . "
			+ "?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>}";*/
	public static String q1_ext = prefix + 
			"SELECT ?X ?Y WHERE"
			+ "{?X rdf:type ub:GraduateStudent . "
			+ "?X ub:takesCourse ?Y . "
			+ "?Y rdf:type ub:GraduateCourse}";
	
	public static String q2 = prefix
			+ "SELECT DISTINCT ?X ?Y ?Z "
			+ "WHERE "
			+ "{?X rdf:type ub:GraduateStudent ."
			+ "?Y rdf:type ub:University . "
			+ "?Z rdf:type ub:Department ."
			+ "?X ub:memberOf ?Z . "
			+ "?Z ub:subOrganizationOf ?Y ."
			+ "?X ub:undergraduateDegreeFrom ?Y}";
	
	public static String q3_ext = prefix + "SELECT ?X ?Y WHERE "
			+ "{?X rdf:type ub:Publication . "
			+ "?X ub:publicationAuthor ?Y . "
			+ "?Y rdf:type ?typeY}";
	
	public static String q4 = prefix + ""
			+ "SELECT ?X ?Y1 ?Y2 ?Y3 "
			+ "WHERE "
			+ "{?X rdf:type ub:AssistantProfessor . "
			+ "?X ub:worksFor <http://www.Department0.University0.edu> . "
			+ "?X ub:name ?Y1 . "
			+ "?X ub:emailAddress ?Y2 . "
			+ "?X ub:telephone ?Y3}" ;
	
	public static String q4_ext = prefix + ""
			+ "SELECT ?X ?Y ?Y1 ?Y2 ?Y3 "
			+ "WHERE "
			+ "{?X rdf:type ub:AssistantProfessor . "
			+ "?X ub:worksFor ?Y . "
			+ "?X ub:name ?Y1 . "
			+ "?X ub:emailAddress ?Y2 . "
			+ "?X ub:telephone ?Y3 . "
			+ "?Y rdf:type ub:Department}" ;
			
	public static String q5 = prefix + "SELECT ?X WHERE {?X rdf:type ub:Person . "
			+ "?X ub:worksFor <http://www.Department0.University0.edu>}";
	
	public static String q5_ext = prefix + "SELECT ?X ?Y WHERE {?X rdf:type ub:Person . "
			+ "?X ub:worksFor ?Y . ?Y rdf:type ub:Department}";
	
	/*public static String q6 = prefix + "SELECT ?X WHERE {?X rdf:type ub:UndergraduateStudent}";*/
	
	public static String q7 = prefix + "SELECT ?X ?Y WHERE  "
			+ "{?X rdf:type ub:UndergraduateStudent . "
			+ "?Y rdf:type ub:Course . "
			+ "?X ub:takesCourse ?Y . "
			+ "<http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y}";
	
	public static String q8 = prefix
			+ "SELECT ?X ?Y ?Z "
			+ "WHERE "
			+ "{?X rdf:type ub:UndergraduateStudent . "
			+ "?Y rdf:type ub:Department . "
			+ "?X ub:worksFor ?Y . "
			+ "?Y ub:subOrganizationOf <http://www.University0.edu> . "
			+ "?X ub:emailAddress ?Z }";
	
	public static String q8_ext = prefix
			+ "SELECT ?X ?Y ?Z "
			+ "WHERE "
			+ "{?X rdf:type ub:UndergraduateStudent . "
			+ "?Y rdf:type ub:Department . "
			+ "?X ub:memberOf ?Y . "
			+ "?Y ub:subOrganizationOf ?Z . "
			+ "?X ub:emailAddress ?X1 }";
	
	public static String q9 = prefix 
			+ "SELECT ?X ?Y ?Z "
			+ "WHERE "
			+ "{"
			+ "?X rdf:type ub:UndergraduateStudent . "
			+ "?Y rdf:type ?someFaculty . "
			+ "?Z rdf:type ub:Course . "
			+ "?X ub:advisor ?Y . "
			+ "?Y ub:teacherOf ?Z . ?X ub:takesCourse ?Z}";
	
	public static String q9_ext = prefix 
			+ "SELECT ?X ?Y ?Z "
			+ "WHERE "
			+ "{"
			+ "?X rdf:type ?x1 . "
			+ "?Y rdf:type ?x2 . "
			+ "?Z rdf:type ?x3 . "
			+ "?X ub:advisor ?Y . "
			+ "?Y ub:teacherOf ?Z . ?X ub:takesCourse ?Z}";
	
	public static String q10 = prefix + "SELECT ?X WHERE "
			+ "{?X rdf:type ub:UndergraduateStudent . "
			+ "?X ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>}";
	
	public static String q10_ext = prefix + "SELECT ?X ?Y WHERE "
			+ "{?X rdf:type ub:Student . "
			+ "?X ub:takesCourse ?Y ."
			+ "?Y rdf:type ub:GraduateCourse . "
			+ "?Y ub:name ?name }";
	
	public static String q11 = prefix + "SELECT ?X WHERE "
			+ "{?X rdf:type ub:ResearchGroup . "
			+ "?X ub:subOrganizationOf <http://www.University0.edu>}";
	
	public static String q11_ext = prefix + "SELECT ?X ?Y WHERE "
			+ "{?X rdf:type ub:ResearchGroup . "
			+ "?X ub:subOrganizationOf ?Y ."
			+ "?Y rdf:type ub:University}";
	
	public static String q12 = prefix + "SELECT ?X ?Y "
			+ "WHERE "
			+ "{?X rdf:type ub:Chair . "
			+ "?Y rdf:type ub:Department . "
			+ "?X ub:memberOf ?Y . "
			+ "?Y ub:subOrganizationOf <http://www.University0.edu>}";
	
	public static String q13 = prefix + "SELECT ?X WHERE "
			+ "{?X rdf:type ?somePerson . "
			+ "<http://www.University0.edu> ub:hasAlumnus ?X}";
	
	public static String q13_ext = prefix + "SELECT ?X ?Y WHERE "
			+ "{?X rdf:type ?somePerson . "
			+ "?Z ub:hasAlumnus ?X . "
			//+ "?X ub:advisor ?Y . "
			+ "?Z rdf:type ?type}";
	
	/*public static String q14 = prefix + "SELECT ?X WHERE {?X rdf:type ub:UndergraduateStudent}";*/
	
	public static String qm1 = prefix + " SELECT DISTINCT ?s ?y ?z ?w WHERE {"
			+ "?s ub:researchInterest ?o2 ; "
			+ " ub:mastersDegreeFrom ?o3 ; "
			+ " ub:doctoralDegreeFrom ?o4 ;"
			+ " ub:memberOf  ?y ;  "
			+ " rdf:type ?o . "
			+ "?y rdf:type ?o5 ; "
			+ "ub:subOrganizationOf ?z . "
			+ " ?z rdf:type ?o6 ; "
			+ " ub:hasAlumnus ?w . "
			+ "?w ub:name ?o8 "
			+ "} ";
	
	public static String qm2 = prefix + " SELECT DISTINCT ?s ?y ?z ?w WHERE {"
			+ "?s ub:researchInterest ?o2 ; "
			+ " ub:mastersDegreeFrom ?o3 ; "			
			+ " ub:emailAddress ?o44 ;"
			+ " ub:worksFor  ?y ;  "
			+ " rdf:type ub:UndergraduateStudent . "
			+ "?y rdf:type ?o5 ; "
			+ "ub:subOrganizationOf ?z . "
			+ " ?z rdf:type ?o6 ; "
			+ " ub:hasAlumnus ?o7 . "
			+ " ?s ub:advisor ?w . "
			+ "?w rdf:type ?o88"
			+ "} ";
	
	public static String qm3 = prefix + " SELECT DISTINCT ?s ?y ?z ?course WHERE {"
			+ "?s ub:researchInterest ?o2 ; "
			+ " ub:mastersDegreeFrom ?o3 ; "			
			+ " ub:emailAddress ?o44 ;"
			+ " ub:worksFor  ?y ;  "
			+ " ub:teacherOf ?course ;"
			+ " rdf:type ?profType . "
			/*+ "?y rdf:type ?o5 ; "
			+ "ub:subOrganizationOf ?z . "*/
			/*+ " ?z rdf:type ?o6 ; "
			+ " ub:hasAlumnus ?o7 . "*/
			+ " ?course rdf:type ?courseType ."
			+ " ?course ub:name ?courseName ." 
			+ " ?student ub:takesCourse ?course . "
			+ " ?student rdf:type ub:UndergraduateStudent . "
			+ " ?student ub:memberOf ?sm ."
			+ " ?sm rdf:type ?smType ."
			+ "} ";
	
	public static String qm4 = prefix + ""
			+ "SELECT DISTINCT ?s1 ?pub ?dept WHERE { "
				//+ "?s1 rdf:type ub:GraduateStudent . "
				+ "?s1 rdf:type ?studentType . "
				+ "?s1 ub:undergraduateDegreeFrom ?uguni . "
				+ "?s1 ub:worksFor ?dept . "				
				//+ "?dept rdf:type ub:Department . "
				+ "?dept rdf:type ?deptType . "
				+ "?dept ub:subOrganizationOf ?sub . "
				//+ "?uguni rdf:type ub:University . "
				//+ "?pub rdf:type ub:Publication . "
				+ "?pub rdf:type ?pubtype . "
				+ "?pub ub:publicationAuthor ?s1 . " 
				+ "}";
	
	public static String qm4_new = prefix + ""
			+ "SELECT DISTINCT ?s1 ?pub ?dept WHERE { "
				//+ "?s1 rdf:type ub:GraduateStudent . "
				+ "?s1 rdf:type ?studentType . "
				+ "?s1 ub:undergraduateDegreeFrom ?uguni . "
				+ "?s1 ub:worksFor ?dept . "				
				+ "?dept rdf:type ub:Department . "
				+ "?dept rdf:type ?deptType . "
				+ "?dept ub:subOrganizationOf ?sub . "				
				+ "?sub rdf:type ub:University . "
				+ "?pub rdf:type ub:Publication . "
				+ "?pub rdf:type ?pubtype . "
				+ "?pub ub:publicationAuthor ?s1 . "
				+ "?s1 ub:advisor ?teacher ."
				+ "?teacher rdf:type ub:Professor ."
				+ "?teacher ub:name ?tname ."
				+ "?teacher ub:doctoralDegreeFrom ?tdf ."
				+ "}";
	
	public static String qm4_ext = prefix + ""
			+ "SELECT DISTINCT ?s1 ?pub ?dept WHERE { "
				+ "?s1 rdf:type ub:GraduateStudent . "				
				+ "?s1 ub:undergraduateDegreeFrom ?uguni . "
				+ "?s1 ub:worksFor ?dept . "				
				+ "?dept rdf:type ub:Department . "				
				+ "?dept ub:subOrganizationOf ?sub . "				
				+ "?pub rdf:type ub:Publication . "
				+ "?pub rdf:type ?pubtype . "
				+ "?pub ub:publicationAuthor ?s1 . " 
				+ "}";
	
	public static String qa10 = "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> "
			+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  "
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "SELECT ?teacher ?dept ?course ?adv ?uName WHERE {  "
			+ "	?teacher rdf:type ?fac. "
			+ "?teacher ub:worksFor ?dept. "
			+ "?student ub:worksFor ?dept. "
			+ "?student ub:takesCourse ?course. "
			+ "?tAsst ub:teachingAssistantOf ?course. "
			+ "?tAsst ub:advisor ?adv. "
			+ "?adv ub:doctoralDegreeFrom "
			+ "?unv. ?unv ub:name ?uName. }";
	
	
	public static String reactomePrefixes = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "PREFIX owl: <http://www.w3.org/2002/07/owl#> "
			+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
			+ "PREFIX dc: <http://purl.org/dc/elements/1.1/> "
			+ "PREFIX dcterms: <http://purl.org/dc/terms/> "
			+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
			+ "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
			+ "PREFIX biopax3: <http://www.biopax.org/release/biopax-level3.owl#>";

	public static String r1 = "SELECT DISTINCT ?pathway ?organism  "
			+ "WHERE "
			+ "{ "
			+ "?pathway rdf:type ?pathType .  "			
			+ "?pathway biopax3:organism ?organism . "
			+ "?organism biopax3:name ?organismName . "
			+ "?organism rdf:type ?orgType. "
			+ "?organism biopax3:xref ?ref . "
			+ "?ref biopax3:id ?id ; "
			+ " rdf:type ?refType "
			+ "} ";
	
	public static String r2 = "SELECT DISTINCT ?pathway ?organism ?ref "
			+ "WHERE "
			+ "{ "
			+ "?pathway rdf:type biopax3:Pathway .  "
			+ "?pathway biopax3:displayName ?pathwayname .  "
			+ "?pathway biopax3:organism ?organism . "
			+ "?organism biopax3:name ?organismName . "
			+ "?organism rdf:type ?orgType. "
			+ "?organism biopax3:xref ?ref . "
			+ "?ref biopax3:id ?id ; "
			+ " rdf:type ?refType "
			+ "} ";
	
	public static String r3 = "SELECT DISTINCT ?organism ?ref "
			+ "WHERE "
			+ "{ "			
			+ "?organism biopax3:name ?organismName . "
			+ "?organism rdf:type ?orgType. "
			+ "?organism biopax3:xref ?ref . "
			+ "?ref biopax3:id ?id ; "
			+ "rdf:type ?refType "
			+ "} ";
	
	public static String r4 = "SELECT DISTINCT ?pathway ?reaction ?entity "
			+ "WHERE  "
			+ "{?pathway rdf:type biopax3:Pathway . "
			+ "?pathway biopax3:displayName ?pathwayname . "
			+ "?pathway biopax3:pathwayComponent ?reaction . "
			+ "?reaction rdf:type biopax3:BiochemicalReaction . "
			+ "?reaction biopax3:left ?entity . "
			+ "?entity biopax3:cellularLocation <http://purl.obolibrary.org/obo/GO_0005886> . "			
			+ "}";
	
	public static String r5 = "SELECT DISTINCT ?pathway ?reaction ?entity "
			+ "WHERE  "
			+ "{?pathway rdf:type biopax3:Pathway . "
			+ "?pathway biopax3:displayName ?pathwayname . "
			+ "?pathway biopax3:pathwayComponent ?reaction . "
			+ "?reaction rdf:type biopax3:BiochemicalReaction . "
			+ "?reaction biopax3:left ?entity . "
			+ "?entity biopax3:cellularLocation <http://purl.obolibrary.org/obo/GO_0005886> . "
			+ "?pathway biopax3:dataSource ?source . "
			+ "?source biopax3:name ?sourceName ."
			+ "}";
	
	public static String r6 = "SELECT DISTINCT ?pathway ?reaction ?entity "
			+ "WHERE  "
			+ "{?pathway rdf:type biopax3:Pathway . "
			+ "?pathway biopax3:displayName ?pathwayname . "
			+ "?pathway biopax3:pathwayComponent ?reaction1 . "
			+ "?reaction1 rdf:type ?reaction1Type . "
			+ "?reaction1 biopax3:pathwayComponent ?reaction2 . "
			+ "?reaction2 rdf:type ?reaction2Type . "
			+ "?reaction2 biopax3:pathwayComponent ?reaction . "
			+ "?reaction rdf:type biopax3:BiochemicalReaction . "
			+ "?reaction biopax3:left ?entity . "
			+ "?entity biopax3:cellularLocation <http://purl.obolibrary.org/obo/GO_0005886> . "
			+ "?pathway biopax3:dataSource ?source . "
			+ "?source biopax3:name ?sourceName ."
			+ "}";
	
	public static String r7 = "SELECT DISTINCT ?organism ?ref "
			+ "WHERE "
			+ "{ "			
			+ "?organism biopax3:name ?organismName . "
			+ "?organism rdf:type ?orgType. "
			+ "?organism biopax3:xref ?ref . "
			+ "?ref biopax3:id ?id ; "
			+ "rdf:type ?refType "
			+ "} ";
	
	public static String r8 = "SELECT DISTINCT ?x "
			+ "WHERE "
			+ "{ "			
			+ "?x biopax3:dataSource ?x1 . "
			+ "?x biopax3:organism ?x2 . "
			+ "?x biopax3:pathwayComponent ?x3 . "
			+ "?x biopax3:comment ?x4 . "
			+ "?x biopax3:evidence ?x5 . "
			+ "?x5 ?p ?o . "
			+ "} ";
	
	public static String r9 = " SELECT ?pathway ?reaction ?complex ?protein  "
			+ "WHERE  "
			+ "{?pathway rdf:type biopax3:Pathway .  "
			+ "?pathway biopax3:displayName ?pathwayname ."
			+ "?pathway biopax3:pathwayComponent ?reaction . "
			+ "?reaction rdf:type biopax3:BiochemicalReaction .  "
			+ "?reaction  ?rel  ?complex . "
			+ "?reaction biopax3:left ?left ."
			+ "?complex rdf:type biopax3:Complex .  "
			+ "?complex biopax3:component ?protein . "
			+ "?protein rdf:type biopax3:Protein . "
			+ "?protein biopax3:entityReference <http://purl.uniprot.org/uniprot/P01308>"
			+ "}";
	
	public static String r10 = " SELECT DISTINCT ?pathway ?reaction ?complex ?protein ?ref  "
			+ "WHERE  "
			+ "{?pathway rdf:type biopax3:Pathway .  "
			+ "?pathway biopax3:displayName ?pathwayname ."
			+ "?pathway biopax3:pathwayComponent ?reaction . "
			+ "?reaction rdf:type biopax3:BiochemicalReaction .  "			
			+ "?reaction biopax3:right ?complex ."
			+ "?complex rdf:type biopax3:Complex .  "
			+ "?complex biopax3:component ?protein . "
			+ "?protein rdf:type biopax3:Protein . "
			+ "?protein biopax3:entityReference ?ref ."
			+ "?ref biopax3:id ?id ; rdf:type ?refType" 
			+ "}";
	
	public static String r11 = "SELECT DISTINCT ?organism ?ref "
			+ "WHERE "
			+ "{ "			
			+ "?organism biopax3:name ?organismName . "
			+ "?organism rdf:type ?orgType. "
			+ "?organism biopax3:xref ?ref . "
			+ "?ref biopax3:id ?id ; "
			+ "rdf:type ?refType ."
			//+ "?organism "
			+ "} ";
	
	public static String r12 = "SELECT DISTINCT * "
			+ "WHERE   "
			+ "{ "
			+ "?x biopax3:pathwayComponent ?pw1 . "
			+ "?pw1 rdf:type ?type1 . "
			+ "?pw1 biopax3:pathwayComponent ?pw2 . "
			+ "?pw2 rdf:type ?type2 . "
			+ "?pw2 biopax3:pathwayComponent ?pw3 . "
			+ "?pw3 rdf:type ?type3 . "
			+ "?pw3 biopax3:pathwayComponent ?pw4 . "
			+ "?pw4 rdf:type ?type4 . "
			+ "?pw2 biopax3:pathwayComponent ?pw5 .   "
			+ "?pw5 rdf:type ?type5 . "
			+ "}     ";
	
	public static String geonamesPrefixes = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
			+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+ "PREFIX owl: <http://www.w3.org/2002/07/owl#> "
			+ "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
			+ "PREFIX dc: <http://purl.org/dc/elements/1.1/> "
			+ "PREFIX dcterms: <http://purl.org/dc/terms/> "
			+ "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
			+ "PREFIX gn: <http://www.geonames.org/ontology#> ";
	
	public static String g1 = "SELECT ?f1 ?f2 ?f3 WHERE "
			+ "{"
			+ "?f1 rdf:type ?ft1 ; "
			+ "gn:parentFeature ?f2 ;"
			+ "gn:postalCode ?fp1 ."
			+ "?f2 rdf:type ?ft2 ; "
			+ "gn:parentFeature ?f3 ."
			+ "?f3 rdf:type ?ft3 ."
			+ "}";
	
	public static String g2 = "SELECT ?f1 ?f2 ?f3 WHERE "
			+ "{"
			+ "?f1 rdf:type ?ft1 ; "
			+ "gn:parentFeature ?f2 ;"
			+ "gn:postalCode ?fp1 ."
			+ "?f2 rdf:type ?ft2 ; "
			+ "gn:parentFeature ?f3 ."
			+ "?f3 rdf:type ?ft3 ."
			+ "?f1 gn:parentCountry ?f3 . "
			//+ "?f4 rdf:type ?ft4 ."
			+ "}";
	
	public static String g3 = "SELECT ?f1 ?f2 ?f3 ?4 WHERE "
			+ "{"
			+ "?f1 rdf:type ?ft1 ; "
			+ "gn:parentFeature ?f2 ;"
			+ "gn:postalCode ?fp1 ;"
			+ "gn:parentADM4 ?fadm1 ."
			+ "?f2 rdf:type ?ft2 ; "
			+ "gn:parentFeature ?f3 ."
			+ "?f2 gn:parentADM3 ?fadm2 ."
			+ "?f3 rdf:type ?ft3 ; gn:wikipediaArticle ?fwiki3 ."
			//+ "?f1 gn:parentFeature ?f3 . "
			+ "?f3 gn:parentFeature ?f4 ."
			+ "?f4 rdf:type ?ft4 ."
			+ "}";
	
	public static String g4 = "SELECT ?f1 ?f2 ?f3 ?4 WHERE "
			+ "{"
			+ "?f1 rdf:type ?ft1 ; "
			+ "gn:parentFeature ?f2 ;"
			+ "gn:postalCode ?fp1 ."
			+ "?f2 rdf:type ?ft2 ; "
			+ "gn:parentFeature ?f3 ."
			+ "?f3 rdf:type ?ft3 ."
			+ "?f1 gn:parentFeature ?f3 . "
			+ "?f3 gn:parentFeature ?f4 ."
			+ "?f4 rdf:type ?ft4 ."
			+ "}";
	
	public static String g5 = " SELECT ?f1 ?f2 ?f3 ?adm1 ?fadm1 WHERE {"
			+ "?f1 rdf:type gn:Feature ; "
			+ " gn:parentFeature ?f2 ;"
			+ " gn:postalCode ?fp1 ."
			+ " ?f2 rdf:type ?ft2 ; "
			+ " gn:parentFeature ?f3 ."
			+ " ?f3 rdf:type ?ft3 . "
			+ " ?f1 gn:parentADM1 ?adm1 . "
			+ " ?adm1 ?p ?fadm1 . "
			+ " ?fadm1 a gn:Feature"
			+ "}";
	
	//very low selectivity in this one
	public static String g6 = " SELECT ?f1 ?adm1 WHERE {" 
			+ " ?f1 rdf:type gn:Feature ;"
			+ " gn:parentADM1 ?adm1 . "
			+ " ?adm1 gn:parentCountry ?fadm1 . "
			+ "}"; 
	
}
