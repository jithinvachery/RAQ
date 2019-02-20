import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.Map.Entry;


/**
 * Print the different statistics 
 * @throws IOException 
 */
class IMDBStats{
	HashMap<String, Integer> yobCountMap 					= new HashMap<>();
	HashMap<String, Integer> yobOnlyCountMap				= new HashMap<>();
	HashMap<String, Integer> genreCountMap 					= new HashMap<>();
	HashMap<String, Integer> genreOnlyCountMap 				= new HashMap<>();
	HashMap<String, Integer> medianRatingCountMap 			= new HashMap<>();
	HashMap<String, Integer> medianOlnyRatingCountMap		= new HashMap<>();
	HashMap<String, Integer> medianRatingActualCountMap		= new HashMap<>();
	HashMap<String, Integer> numCommonMoviesCountMap 		= new HashMap<>();
	HashMap<String, Integer> primaryProfessionCountMap 		= new HashMap<>();
	HashMap<String, Integer> primaryProfessionOnlyCountMap 	= new HashMap<>();
}

/**
 * Handles the IMDB dataset
 * @author jithin
 *
 */

class IMDBFeatures extends NodeFeatures {
	//features
	final int age; //0 if we donot know the value
	final HashSet<Integer> genre; //top 3
	final int isAlive;  //1 alive 0 dead 2 donot know
	final int numMovies;
	final int numTitles;
	final HashSet<Integer> professions; //top 3
	final int primaryProfession; //top 1
	final int medianRating; //multiplied by 10
	final int isHollywood; //1 hollywood 0 bollywood

	final int numMoviesOscar;
	final int numMoviesOscarNominations;
	final int numOscar;
	final int numOscarNominations;
	
	final int numMoviesFilmFare;
	final int numMoviesFilmFareNominations;
	final int numFilmFare;
	final int numFilmFareNominations;

	final int numMoviesAward;
	final int numMoviesAwardNominations;
	final int numAward;
	final int numAwardNominations;

	final int yob; //used only for probability 
	
	static final int ageBucketSize = 5;
	static final int numBucketSize = 5;
	
	static int maxNumMovies = 0;
	static int maxNumTitles = 0;
	//this feature is not a feature but an info
	final String personID;
	final int ageActual;
	final private int numMoviesActual;
	final private int numTitlesActual;
	final int medianRatingActual;
	final HashSet<Integer> movies;
	final int yobActual;
	final static double maxCommonMovies = 400.0; //maximum number of common movies between 2 people
	
	static private final Genre[] genreList =  Genre.values();;
	static private final Professions[] professionList =  Professions.values();;
	
	static final int numCategoricalFeatures    = IMDB.numCategoricalFeatures;
	static final int numNonCategoricalFeatures = IMDB.numFeatures - IMDB.numCategoricalFeatures;

	public final static int numFeatures = IMDB.numFeatures;
	
	public IMDBFeatures(NameEntry nameEntry) {
		personID = nameEntry.id;
		int age_1,ageActual_1;
		if (nameEntry.birthYear > 0) {
			if (nameEntry.deathYear > 0) {
				ageActual_1 = (nameEntry.deathYear - nameEntry.birthYear);
				age_1 = ageActual_1/ageBucketSize;
				isAlive = 0;
			} else {
				ageActual_1 = (2018 - nameEntry.birthYear); //datset was downloaded in 2018
				age_1 = ageActual_1/ageBucketSize;
				isAlive = 1;
			}
		} else {
			age_1 = ageActual_1 = 0;
			isAlive = 0;
		}
		if ((ageActual_1 < 0) || (ageActual_1 > 110))
			age_1 = ageActual_1 = 0;
		age = age_1;
		ageActual = ageActual_1;
		
		genre 				= nameEntry.genreMovie;
		numMoviesActual 	= nameEntry.movies.size();
		numMovies 			= numMoviesActual/numBucketSize;
		numTitlesActual 	= nameEntry.titles.size();
		numTitles 			= numTitlesActual/numBucketSize;
		professions			= nameEntry.primaryProfessions;
		medianRatingActual	= nameEntry.medianRating_movies;
		medianRating		= medianRatingActual/numBucketSize;
		isHollywood			= nameEntry.isHollywood;
		primaryProfession	= nameEntry.primaryProfession;
		movies				= nameEntry.movies;

		numOscar					= nameEntry.numOscar;
		numOscarNominations			= nameEntry.numOscarNominations;		
		numMoviesOscar				= nameEntry.numMoviesOscar;
		numMoviesOscarNominations	= nameEntry.numMoviesOscarNominations;

		numFilmFare					= nameEntry.numFilmFare;
		numFilmFareNominations		= nameEntry.numFilmFareNominations;		
		numMoviesFilmFare			= nameEntry.numMoviesFilmFare;
		numMoviesFilmFareNominations= nameEntry.numMoviesFilmFareNominations;

		numAward 					= numOscar 			 		+ numFilmFare;
		numAwardNominations			= numOscarNominations 		+ numFilmFareNominations;
		numMoviesAward 				= numMoviesOscar 			+ numMoviesFilmFare;
		numMoviesAwardNominations 	= numMoviesOscarNominations + numMoviesFilmFareNominations;

		if (nameEntry.birthYear > 0)
			yob 					= (2018 - nameEntry.birthYear)/ageBucketSize;
		else
			yob						= 0;

		yobActual 					= nameEntry.birthYear;
	}

	@Override
	String[] AllFeatures() {
		return IMDB.allFeaturesStrings;
	}

	@Override
	ArrayList<ArrayList<String>> AllFeatureValues(NodeFeatures features2) {
		IMDBFeatures neighbourFeature = (IMDBFeatures)features2;
		
		ArrayList<ArrayList<String>> ret = new ArrayList<>(numFeatures);

		for (int i=0; i<numFeatures; i++) {
			ArrayList<String> s = new ArrayList<>(); 
			ret.add(s);
		}
		
		do {
			boolean diGraph = false;
			ArrayList<String> S;
			//all this mombo jumbo  of using switch case, to ensure we do not miss out a feature
			
			for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.valuesICE()) {
				int i = feat.ordinalICE();
				S = ret.get(i);
				switch (feat) {
				case age:
					S.add(getString(age, neighbourFeature.age, diGraph));
					break;
				case genre:
					for (int g1 : genre) {
						for (int g2 : neighbourFeature.genre) {
							S.add(getString(g1, g2, diGraph));
						}
					}
					break;
				case isAlive:
					S.add(getString(isAlive, neighbourFeature.isAlive, diGraph));
					break;
				case medianRating:
					S.add(getString(medianRating, neighbourFeature.medianRating, diGraph));
					break;
				case numMovies:
					S.add(getString(numMovies, neighbourFeature.numMovies, diGraph));
					break;
				case numTitles:
					S.add(getString(numTitles, neighbourFeature.numTitles, diGraph));
					break;
				case professions:
					for (int p1 : professions) {
						for (int p2 : neighbourFeature.professions) {
							S.add(getString(p1, p2, diGraph));
						}
					}
					break;
				case hollywood:
					S.add(getString(isHollywood, neighbourFeature.isHollywood, diGraph));
					break;
				case primaryProfession:
					S.add(getString(primaryProfession, neighbourFeature.primaryProfession, diGraph));
					break;
				case numCommonMovies:
					//NOTE
					//we are deviating from the norm
					HashSet<Integer> m = new HashSet<>(movies);
					m.retainAll(neighbourFeature.movies);
					
					S.add(getString(m.size(), m.size(), diGraph));
					break;
				case numMoviesOscar:
					S.add(getString(numMoviesOscar, neighbourFeature.numMoviesOscar, diGraph));
					break;
				case numMoviesOscarNominations:
					S.add(getString(numMoviesOscarNominations, neighbourFeature.numMoviesOscarNominations, diGraph));
					break;
				case numOscar:
					S.add(getString(numOscar, neighbourFeature.numOscar, diGraph));
					break;
				case numOscarNominations:
					S.add(getString(numOscarNominations, neighbourFeature.numOscarNominations, diGraph));
					break;
				case yob:
					S.add(getString(yob, neighbourFeature.yob, diGraph));
					break;
				case numMoviesFilmFare:
					S.add(getString(numMoviesFilmFare, neighbourFeature.numMoviesFilmFare, diGraph));
					break;
				case numMoviesFilmFareNominations:
					S.add(getString(numMoviesFilmFareNominations, neighbourFeature.numMoviesFilmFareNominations, diGraph));
					break;
				case numFilmFare:
					S.add(getString(numFilmFare, neighbourFeature.numFilmFare, diGraph));
					break;
				case numFilmFareNominations:
					S.add(getString(numFilmFareNominations, neighbourFeature.numFilmFareNominations, diGraph));
					break;
				case numMoviesAward:
					S.add(getString(numMoviesAward, neighbourFeature.numMoviesAward, diGraph));
					break;
				case numMoviesAwardNominations:
					S.add(getString(numMoviesAwardNominations, neighbourFeature.numMoviesAwardNominations, diGraph));
					break;
				case numAward:
					S.add(getString(numAward, neighbourFeature.numAward, diGraph));
					break;
				case numAwardNominations:
					S.add(getString(numAwardNominations, neighbourFeature.numAwardNominations, diGraph));
					break;
				}
			}
		} while (false);
		
		return ret;
	}

	@Override
	int NumFeatures() {
		return numFeatures;
	}

	@Override
	int NumCategoricalFeatures() {
		return numCategoricalFeatures;
	}
	@Override
	int NumNonCategoricalFeatures() {
		return numNonCategoricalFeatures;
	}

	@Override
	int[][] getSummaryVector(double[] ve) {
		//allocating memory
		int[][] neighbourhoodSummary = new int[NumFeatures()][BFSQuery.HeuristicsNumBin];

		//summarize the values take by feature
		//all this mombo jumbo  of using switch case, to ensure we do not miss out a feature
		int i, j;
		
		for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.valuesICE()) {
			i = feat.ordinalICE();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		}
		return neighbourhoodSummary;
	}

	@Override
	double[] GetVE(NodeFeatures features) {
		double[] ve = new double[numFeatures];
		IMDBFeatures nf = (IMDBFeatures)features;
		     
		//processing non-categorical features
		HashSet<Integer> intersection, union;
		
		for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.valuesICE()) {
			switch (feat) {
			case age:
				GetVeHelperZeroNotGood(ve, age, nf.age, feat.ordinalICE());
				break;
			case genre:
				intersection = new HashSet<>(genre);
				union		 = new HashSet<>(genre);
				intersection.retainAll(nf.genre);
				union.addAll(nf.genre);

				GetVeHelper(ve, intersection.size(), union.size(), feat.ordinalICE());
				break;
			case isAlive:
				if (isAlive == nf.isAlive)
					ve[feat.ordinalICE()]=1.0;
				else
					ve[feat.ordinalICE()]=0;
				break;
			case medianRating:
				GetVeHelperZeroNotGood(ve, medianRating, nf.medianRating, feat.ordinalICE());
				break;
			case numMovies:
				GetVeHelperZeroNotGood(ve, numMovies, nf.numMovies, feat.ordinalICE());
				break;
			case numTitles:
				GetVeHelperZeroNotGood(ve, numTitles, nf.numTitles, feat.ordinalICE());
				break;
			case professions:
				intersection = new HashSet<>(professions);
				union		 = new HashSet<>(professions);
				intersection.retainAll(nf.professions);
				union.addAll(nf.professions);

				GetVeHelper(ve, intersection.size(), union.size(), feat.ordinalICE());
				break;
			case hollywood:
				if (isHollywood == nf.isHollywood)
					ve[feat.ordinalICE()]=1.0;
				else
					ve[feat.ordinalICE()]=0;
				break;
			case primaryProfession:
				if (primaryProfession == nf.primaryProfession)
					ve[feat.ordinalICE()]=1.0;
				else
					ve[feat.ordinalICE()]=0;
				break;
			case numCommonMovies:
				HashSet<Integer> m = new HashSet<>(movies);
				m.retainAll(nf.movies);
				GetVeHelper(ve, m.size(), maxCommonMovies , feat.ordinalICE());
				break;
			case numMoviesOscar:
				GetVeHelperZeroNotGood(ve, numMoviesOscar, nf.numMoviesOscar, feat.ordinalICE());
				break;
			case numMoviesOscarNominations:
				GetVeHelperZeroNotGood(ve, numMoviesOscarNominations, nf.numMoviesOscarNominations, feat.ordinalICE());
				break;
			case numOscar:
				GetVeHelperZeroNotGood(ve, numOscar, nf.numOscar, feat.ordinalICE());
				break;
			case numOscarNominations:
				GetVeHelperZeroNotGood(ve, numOscarNominations, nf.numOscarNominations, feat.ordinalICE());
				break;
			case yob:
				//the ve vector is based on the difference in age.
				// the max difference in age is set at 100
				/*
				int diff = yobActual - nf.yobActual;
				if (diff < 0)
					diff *= -1;
				
				GetVeHelper(ve, diff, 100, feat.ordinalICE());
				*/
				GetVeHelper(ve, yobActual, nf.yobActual, feat.ordinalICE());
				break;
			case numMoviesFilmFare:
				GetVeHelperZeroNotGood(ve, numMoviesFilmFare, nf.numMoviesFilmFare, feat.ordinalICE());
				break;
			case numMoviesFilmFareNominations:
				GetVeHelperZeroNotGood(ve, numMoviesFilmFareNominations, nf.numMoviesFilmFareNominations, feat.ordinalICE());
				break;
			case numFilmFare:
				GetVeHelperZeroNotGood(ve, numFilmFare, nf.numFilmFare, feat.ordinalICE());
				break;
			case numFilmFareNominations:
				GetVeHelperZeroNotGood(ve, numFilmFareNominations, nf.numFilmFareNominations, feat.ordinalICE());
				break;
				
			case numMoviesAward:
				GetVeHelperZeroNotGood(ve, numMoviesAward, nf.numMoviesAward, feat.ordinalICE());
				break;
			case numMoviesAwardNominations:
				GetVeHelperZeroNotGood(ve, numMoviesAwardNominations, nf.numMoviesAwardNominations, feat.ordinalICE());
				break;
			case numAward:
				GetVeHelperZeroNotGood(ve, numAward, nf.numAward, feat.ordinalICE());
				break;
			case numAwardNominations:
				GetVeHelperZeroNotGood(ve, numAwardNominations, nf.numAwardNominations, feat.ordinalICE());
				break;
			}
		}
		return ve;
	}

	@Override
	public void Print() {
		System.out.println("Name        				: "+name());
		for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.valuesICE()) {
			switch (feat) {
			case age:
				System.out.println("age actual  				: "+ageActual);
				System.out.println("age         				: "+age);
				break;
			case yob:
				System.out.println("YOB actual  				: "+yobActual);
				System.out.println("YOB         				: "+yob);
				break;
			case genre:
				System.out.println("genre       				: "+genreString());
				break;
			case isAlive:
				System.out.println("isAlive     				: "+isAlive);
				break;
			case medianRating:
				System.out.println("medianRating				: "+medianRating);
				break;
			case numMovies:
				System.out.println("numMovies actual			: "+numMoviesActual);
				System.out.println("numMovies   				: "+numMovies);
				break;
			case numTitles:
				System.out.println("numTitles   				: "+numTitlesActual);
				break;
			case professions:
				System.out.println("professions 				: "+professionsString());
				break;
			case hollywood:
				System.out.println("hollywood   				: "+isHollywood);
				break;
			case primaryProfession:
				System.out.println("primary Profession			: "+ primaryProfessionString());
				break;
			case numCommonMovies:
				//nothing to be done
				break;
			case numMoviesOscar:
				System.out.println("numMoviesOscar   			: "+numMoviesOscar);
				break;
			case numMoviesOscarNominations:
				System.out.println("numMoviesOscarNominations   : "+numMoviesOscarNominations);
				break;
			case numOscar:
				System.out.println("numOscar   					: "+numOscar);
				break;
			case numOscarNominations:
				System.out.println("numOscarNominations   		: "+numOscarNominations);
				break;

			case numMoviesFilmFare:
				System.out.println("numMoviesFilmFare  			: "+numMoviesFilmFare);
				break;
			case numMoviesFilmFareNominations:
				System.out.println("numMoviesFilmFareNominations: "+numMoviesFilmFareNominations);
				break;
			case numFilmFare:
				System.out.println("numFilmFare   				: "+numFilmFare);
				break;
			case numFilmFareNominations:
				System.out.println("numFilmFareNominations   	: "+numFilmFareNominations);
				break;

			case numMoviesAward:
				System.out.println("numMoviesAward   			: "+numMoviesAward);
				break;
			case numMoviesAwardNominations:
				System.out.println("numMoviesAwardNominations   : "+numMoviesAwardNominations);
				break;
			case numAward:
				System.out.println("numAward   					: "+numAward);
				break;
			case numAwardNominations:
				System.out.println("numAwardNominations   		: "+numAwardNominations);
				break;
			}
		}
	}

	String primaryProfessionString() {
		return ProfessionCategory.values()[primaryProfession].toString();
	}

	String professionsString() {
		String ret="";
		
		for (Integer p : professions) {
			ret+=professionList[p]+":";
		}
		
		return ret;
	}

	String genreString() {
		String ret="";
		
		for (Integer g : genre) {
			ret+=genreList [g]+":";
		}
		
		return ret;
	}

	@Override
	public void PrintCSV() {
		System.out.println(csv());
	}

	private String csv() {
		String s = "=HYPERLINK(\"https://www.imdb.com/name/"+personID+"\"),";
		s+=name()+":"+"";

		boolean validFeature = true;
		for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.valuesICE()) {
			s = csvHelper(s, feat, validFeature);
		}		
		
		/*
		validFeature = false;
		for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.nonFeatures) {
			s = csvHelper(s, feat, validFeature);
		}
		*/		
		
		return s;
	}

	private String csvHelper(String s, IMDB.allFeaturesICE feat, boolean validFeature) {
		if (!validFeature)
			s+="* ";
		switch (feat) {
		case age:
			s += ageActual;
			break;
		case yob:
			s += yobActual;
			break;
		case genre:
			s += genreString();
			break;
		case isAlive:
			s += isAlive;
			break;
		case medianRating:
			s += medianRatingActual;
			break;
		case numMovies:
			s += numMoviesActual;
			break;
		case numTitles:
			s += numTitlesActual;
			break;
		case professions:
			s += professionsString();
			break;
		case hollywood:
			s += isHollywood;
			break;
		case primaryProfession:
			s += primaryProfessionString();
			break;
		case numCommonMovies:
			//nothing to be done
			break;
		case numMoviesOscar:
			s += numMoviesOscar;
			break;
		case numMoviesOscarNominations:
			s += numMoviesOscarNominations;
			break;
		case numOscar:
			s += numOscar;
			break;
		case numOscarNominations:
			s += numOscarNominations;
			break;

		case numMoviesFilmFare:
			s += numMoviesFilmFare;
			break;
		case numMoviesFilmFareNominations:
			s += numMoviesFilmFareNominations;
			break;
		case numFilmFare:
			s += numFilmFare;
			break;
		case numFilmFareNominations:
			s += numFilmFareNominations;
			break;	

		case numMoviesAward:
			s += numMoviesAward;
			break;
		case numMoviesAwardNominations:
			s += numMoviesAwardNominations;
			break;
		case numAward:
			s += numAward;
			break;
		case numAwardNominations:
			s += numAwardNominations;
			break;	
		}
		s+=",";
		return s;
	}

	@Override
	public void PrintCSVHeader() {
		System.out.println(csvHeader());
	}

	private String csvHeader() {
		String s="link,";
		
		for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.valuesICE()) {
			s+=feat+",";
		}
		
		/*
		for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.nonFeatures) {
			s+="* "+feat+",";
		}		
		*/
		return s;
	}

	@Override
	public void PrintCSVToFile(FileWriter fooWriter) throws IOException {
		fooWriter.write(csv());
		fooWriter.write("\n");
	}

	@Override
	public void PrintCSVHeaderToFile(FileWriter fooWriter) throws IOException {
		fooWriter.write(csvHeader());
		fooWriter.write("\n");
	}

	@Override
	public double Distance(NodeFeatures tFeatures) {
		double distance = 0.0;

		//add a cast
		IMDBFeatures tfeat = (IMDBFeatures) tFeatures;

		HashSet<Integer> intersection;	
		HashSet<Integer> union;

		int n = numFeatures;
		
		for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.valuesICE()) {
			switch (feat) {
			case age:
				distance += (1.0-NodeFeatures.MinByMax(age, tfeat.age));
				break;
			case yob:
				distance += (1.0-NodeFeatures.MinByMax(yob, tfeat.yob));
				break;
			case genre:
				intersection = new HashSet<>(genre);
				intersection.retainAll(tfeat.genre);	
				union = new HashSet<>(genre);
				union.addAll(tfeat.genre);
				distance += (1.0-NodeFeatures.MinByMax(intersection.size(), union.size()));
				break;
			case isAlive:
				if (isAlive != tfeat.isAlive)
					distance += 1;
				break;
			case medianRating:
				distance += (1.0-NodeFeatures.MinByMax(medianRating, tfeat.medianRating));
				break;
			case numMovies:
				distance += (1.0-NodeFeatures.MinByMax(numMovies, tfeat.numMovies));
				break;
			case numTitles:
				distance += (1.0-NodeFeatures.MinByMax(numTitles, tfeat.numTitles));
				break;
			case professions:
				intersection = new HashSet<>(professions);
				intersection.retainAll(tfeat.professions);	
				union = new HashSet<>(professions);
				union.addAll(tfeat.professions);
				distance += (1.0-NodeFeatures.MinByMax(intersection.size(), union.size()));
				break;
			case hollywood:
				if (isHollywood != tfeat.isHollywood)
					distance += 1;
				break;
			case primaryProfession:
				if (primaryProfession != tfeat.primaryProfession)
					distance += 1;
				break;
			case numCommonMovies:
				//we do not consider this feature in calculation of distance
				//number of movies in common
			{
				HashSet<Integer> set = new HashSet<>(movies);
				set.retainAll(tfeat.movies);
				distance += (1.0 - set.size()/maxCommonMovies);
			}
				break;
			case numMoviesOscar:
				distance += (1.0-NodeFeatures.MinByMax(numMoviesOscar, tfeat.numMoviesOscar));
				break;
			case numMoviesOscarNominations:
				distance += (1.0-NodeFeatures.MinByMax(numMoviesOscarNominations, tfeat.numMoviesOscarNominations));
				break;
			case numOscar:
				distance += (1.0-NodeFeatures.MinByMax(numOscar, tfeat.numOscar));
				break;
			case numOscarNominations:
				distance += (1.0-NodeFeatures.MinByMax(numOscarNominations, tfeat.numOscarNominations));
				break;
			case numMoviesFilmFare:
				distance += (1.0-NodeFeatures.MinByMax(numMoviesFilmFare, tfeat.numMoviesFilmFare));
				break;
			case numMoviesFilmFareNominations:
				distance += (1.0-NodeFeatures.MinByMax(numMoviesFilmFareNominations, tfeat.numMoviesFilmFareNominations));
				break;
			case numFilmFare:
				distance += (1.0-NodeFeatures.MinByMax(numFilmFare, tfeat.numFilmFare));
				break;
			case numFilmFareNominations:
				distance += (1.0-NodeFeatures.MinByMax(numFilmFareNominations, tfeat.numFilmFareNominations));
				break;
			case numMoviesAward:
				distance += (1.0-NodeFeatures.MinByMax(numMoviesAward, tfeat.numMoviesAward));
				break;
			case numMoviesAwardNominations:
				distance += (1.0-NodeFeatures.MinByMax(numMoviesAwardNominations, tfeat.numMoviesAwardNominations));
				break;
			case numAward:
				distance += (1.0-NodeFeatures.MinByMax(numAward, tfeat.numAward));
				break;
			case numAwardNominations:
				distance += (1.0-NodeFeatures.MinByMax(numAwardNominations, tfeat.numAwardNominations));
				break;
			}
		}

		return distance/n;
	}

	@Override
	public boolean NodeIsSameAs(NodeFeatures tFeatures) {
		IMDBFeatures feat = (IMDBFeatures)tFeatures;
		
		return (personID.compareTo(feat.personID)==0);
	}

	@Override
	public boolean Filter() {
		boolean ret = false;
		
		int g = Genre.Adult.ordinal();
		if (genre.contains(g))
			ret = true;
		
		return ret;
	}


	@Override
	public boolean Filter(Graph.Edge qEdge, Graph.Edge tEdge) {
		boolean ret = false;
		
		Graph<IMDBFeatures>.Edge edge1 = qEdge;
		Graph<IMDBFeatures>.Edge edge2 = tEdge;
		
		//return true if the pair of edges cannot be matched
		
		{
			String id1 = ((IMDBFeatures)edge2.node1.features).personID;
			String id2 = ((IMDBFeatures)edge2.node2.features).personID;
			int i;
			boolean cond1 = false;
			
			switch (id1) {
			case "nm0908094":
				i=0;
			case "nm0003418":
				i=0;
			case "nm0605775":
				cond1 = true;
				break;
				default:
					break;
			}
			
			if (cond1)
			switch (id2) {
			case "nm0908094":
				i=0;
			case "nm0003418":
				i=0;
			case "nm0605775":
				cond1=false;
				break;
				default:
					break;
			}
		}
		
		//we need exact match in primary profession
		int p1 = ((IMDBFeatures)edge1.node1.features).primaryProfession;
		int p2 = ((IMDBFeatures)edge1.node2.features).primaryProfession;

		p1 = convertToEquivalenceClass(p1);
		p2 = convertToEquivalenceClass(p2);
		
		String s1;
		
		if (p1 > p2)
			s1 = p1+","+p2;
		else
			s1 = p2+","+p1;
		
		p1 = ((IMDBFeatures)edge2.node1.features).primaryProfession;
		p2 = ((IMDBFeatures)edge2.node2.features).primaryProfession;

		p1 = convertToEquivalenceClass(p1);
		p2 = convertToEquivalenceClass(p2);		
		
		String s2;
		if (p1 > p2)
			s2 = p1+","+p2;
		else
			s2 = p2+","+p1;
		
		if (s1.compareTo(s2) != 0)
			ret = true;
		
		return ret;
	}

	/**
	 * we have certain professions as equivalence class
	 * actor and actress are same
	 * producer and director are same
	 * @param p1
	 * @return
	 */
	private int convertToEquivalenceClass(int p) {
		//convert a actor to actress
		if (ProfessionCategory.actor.ordinal() == p)
			p = ProfessionCategory.actress.ordinal();

		//convert a director to producer
		else if (ProfessionCategory.director.ordinal() == p)
			p = ProfessionCategory.producer.ordinal();
		
		return p;
	}

	public static int maxMovies() {
		return maxNumMovies;
	}

	public static int maxTitles() {
		return maxNumTitles;
	}

	public static void updateMaxTitles(int numTitles, int numMovies) {
		maxNumMovies = Integer.max(maxNumMovies, numMovies/numBucketSize);
		maxNumTitles = Integer.max(maxNumTitles, numTitles/numBucketSize);
	}

	public String name() {
		return IMDB.nameIDMap.get(personID).primaryName;
	}

	/**
	 * The most importnt feature value
	 * @return
	 */
	public String importantDetail() {
		String ret = "";
		
		ret += numAwardNominations;
		ret += ", "+numMoviesAwardNominations;

		return ret;
	}
	
}

public class IMDB {
	static final int numFeatures 			= allFeaturesICE.lengthICE();
	static final int numCategoricalFeatures = allFeaturesICE.numFeaturesCategorical();
	static final  String[] allFeaturesStrings = allFeaturesICE.strings();

	static private Graph<IMDBFeatures> IMDBGraph;
		
	static HashMap<String,  NameEntry>  nameIDMap   = new HashMap<>();
	static HashMap<Integer, TitleEntry> titleIDMap  = new HashMap<>();
	
	static double[][] Prob;
	static Probability probability;
	
	static private CGQHierarchicalIndex<IMDBFeatures> IMDBCGQhierarchicalIndex;
	static private ArrayList<ArrayList<Graph<IMDBFeatures>.Edge>> queryGraphList;

	//oscar information
	static int maxNomination = 0;
	static int maxNominationMovies = 0;
	private static HashMap<String, Integer> oscarWinnersMap 		= new HashMap<>();
	private static HashMap<String, Integer> oscarNominationsMap		= new HashMap<>();
	private static HashMap<String, Integer> filmFareWinnersMap 		= new HashMap<>();
	private static HashMap<String, Integer> filmFareNominationsMap	= new HashMap<>();
	
	static HashSet<String> oscarWinnersMovies 				= new HashSet<>();
	static HashSet<String> oscarNominationsMovies 			= new HashSet<>();
	static HashSet<String> filmFareWinnersMovies			= new HashSet<>();
	static HashSet<String> filmFareNominationsMovies		= new HashSet<>();
	
	private static final String queryFName = "../data/imdb/imdb.query";
	
	private static final int genreK = 1; //size on number of genre
	
	//this is the order of features used
	enum allFeaturesICE {
		//categorical
		isAlive						(true, true),
		hollywood					(true, true),
		primaryProfession			(true, true),
		
		//non-categorical
		age							(false, false),
		yob							(true,  false), //year of birth
		genre						(true,  false), //top genreK
		numMovies					(false, false),
		numTitles 					(false, false),
		professions 				(false, false), //top 3
		medianRating				(true,  false),

		numOscar					(false, false),
		numOscarNominations			(false, false),
		numMoviesOscar				(false, false),
		numMoviesOscarNominations	(false, false),

		numFilmFare					(false, false),
		numFilmFareNominations		(false, false),
		numMoviesFilmFare			(false, false),
		numMoviesFilmFareNominations(false, false),

		numAward					(false, false),
		numAwardNominations			(false, false),
		numMoviesAward				(false, false),
		numMoviesAwardNominations	(false, false),

		numCommonMovies				(true, false), 
		;
		
		/** should we treat the profession "actress as actor" */
		static boolean treatActressAsActor 		= false;
		/** do we want profession to be given higher weight */
		static boolean setProfessionHighWeight 	= false;
		
		static HashSet<IMDB.allFeaturesICE> features = new HashSet<>();
		static HashSet<IMDB.allFeaturesICE> nonFeatures = new HashSet<>();
		
		final private boolean  valid;
		final private boolean  isCategorical;
				
		static private final int lengthICE = lengthDoNoCall();
		static final private allFeaturesICE[] validValues = valid();
		static private int[] ordinalICE;
				
		static allFeaturesICE[] valuesICE() {
			return validValues;
		}

		public boolean isValid() {
			return valid;
		}

		allFeaturesICE (boolean valid, boolean isCategorical) {
			this.valid 			= valid;
			this.isCategorical 	= isCategorical;
		}
		
		/**
		 * Should always have categorical features as the first features
		 * @return
		 */
		private static allFeaturesICE[] valid() {
			allFeaturesICE [] ret = new allFeaturesICE[lengthICE()];
			ordinalICE = new int[allFeaturesICE.values().length];
			int i=0;
			int j=0;

			for (allFeaturesICE c : values()) {
				if (c.valid && c.isCategorical) {
					ordinalICE[j++] = i;
					features.add(c);

					ret[i++] = c;
				}
			}

			for (allFeaturesICE c : values()) {
				if (!c.valid) {
					ordinalICE[j++] = -1; //should cause crashes, intentional
					nonFeatures.add(c);
				} else if (!c.isCategorical){
					ordinalICE[j++] = i;
					features.add(c);

					ret[i++] = c;
				}
			}
			
			return ret;
		}

		private static int lengthICE() {
			return lengthICE;
		}		
		
		private static int lengthDoNoCall() {
			int i=0;
			
			for (allFeaturesICE c : values()) {
				if (!c.valid)
					continue;
				
				i++;
			}
			
			return i;
		}
		
		int ordinalICE () {
			return ordinalICE[this.ordinal()];
		}
		
		public static String[] strings() {
			String []ret = new String[allFeaturesICE.lengthICE()];

			int i=0;
			for (allFeaturesICE features:allFeaturesICE.valuesICE()) {
				ret[i++] = features.toString();
			}

			return ret;
		}

		public static int numFeaturesCategorical() {
			int ret=0;

			for (allFeaturesICE a : valuesICE()) {
				if (a.isCategorical)
					ret++;
			}			
			return ret;
		}
	}

	static ArrayList<String> globalInput = new ArrayList<>();
	/**
	 * Interactively perform qualitative experiment
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void RunQualitativeInteractive(int K, Scanner reader) throws IOException, InterruptedException, ExecutionException {
		//Initialize the dataset
		/*
		for (Region r : Region.values()) {
			System.out.println("Do you want to consider "+r+"? (enter 1 for yes) ");
			int i = reader.nextInt();
			if (i!=1)
				r.donotConsider();
		}
		 */
		boolean createIndex = true;
		boolean loadQueryGraph=false;
		init(createIndex, loadQueryGraph);
		
		globalInput.clear();
		while(true) {
			int input;
			System.out.print ("Do you want to  create a new query graph ? (enter 0 to exit) : ");
			try {
				input = reader.nextInt();
				globalInput.add(""+input);
			} catch (Exception e) {
				reader.next();
				input = 1;
			}

			if (input == 0)
				break;

			Graph<IMDBFeatures> queryGraph = CreateQueryGraph(reader);
			
			if (queryGraph.SizeNodes() == 0) {
				System.out.println("Empty Query Graph");
				continue;	
			}

			System.out.print ("K : ");
			K = reader.nextInt();
			globalInput.add(""+K);
			reader.nextLine();
			/*
			*/			
			
			if (K < 0) {
				System.out.println("Invalid K : ");
				continue;
			}
			
			System.out.print ("Fname to store the results : ");
			String fname = reader.next();
			
			RunQualitativeHelper (K, "../results/QualitativeIMDB/"+fname, queryGraph);
			if (InterruptSearchSignalHandler.Interrupt()) {
				//we were interrupted from keyboard
				InterruptSearchSignalHandler.ResetFlag(IMDBGraph);
				return;
			}
			
			System.out.println("*******************");
			System.out.println("the input was");
			for (String s : globalInput)
				System.out.println(s);
			globalInput.clear();
		}
	}

	private static void RunQualitativeHelper(int K, String fname,
			Graph<IMDBFeatures> queryGraph)
					throws IOException {
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		//System.out.println("PRESS ENTER to see the query : ");
		//System.in.read();
				
		File myFoo_RAQ 			= new File(fname+"_RAQ.csv");
		File myFoo_uniform 		= new File(fname+"_uniform.csv");
		File myFoo_Traditional 	= new File(fname+"_Traditional.csv");
		
		FileWriter fooWriter_RAQ 		= new FileWriter(myFoo_RAQ, 		false);
		FileWriter fooWriter_uniform 	= new FileWriter(myFoo_uniform, 	false);
		FileWriter fooWriter_Traditional= new FileWriter(myFoo_Traditional, false);

		//Run CGQ
		ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<IMDBFeatures>.Node>>> topK;
		
		//queryGraph.UpdateWeightsWithCap(probability);
		if (IMDB.allFeaturesICE.primaryProfession.isValid() &&
				IMDB.allFeaturesICE.setProfessionHighWeight)
			queryGraph.UpdateWeightsWithHardSetting(probability, 
					IMDB.allFeaturesICE.primaryProfession.ordinalICE(), 0.5);
		else
			queryGraph.UpdateWeights(probability);
		boolean showProgress = false;
		queryGraph.updateNeighbourhoodSummary(showProgress);
		
		showProgress = true;
		boolean fuzzy = false, doNotUseIndex = false, avoidTAHeuristics=true;
		Helper.CallByReference<Integer> depth = null;
		Helper.CallByReference<Long> numDFS = null, numEdgesTouched = null;
		topK = BFSQuery.GetTopKSubgraphsCGQHierarchicalExcludingQueryAndFilterNode
				(K, queryGraph, IMDBGraph, IMDBCGQhierarchicalIndex,
						showProgress, depth, numDFS, numEdgesTouched, 
						fuzzy, avoidTAHeuristics, 
						RAQ.BeamWidth, RAQ.WidthThreshold, doNotUseIndex);
		
		QualitativeHelperHelper (queryGraph, topK, fooWriter_RAQ);
		fooWriter_RAQ.close();
		
		/*
		//Run Traditional query
		topK = TraditionalQuery.GetTopKSubgraphsCGQHierarchicalExcludingQueryAndFilterNode
		(K, queryGraph, IMDBGraph, true, true);
		QualitativeHelperHelper (queryGraph, topK, fooWriter_Traditional);
		fooWriter_Traditional.close();

		//uniform weights
		queryGraph.UpdateWeightsUniformly();
		topK = BFSQuery.GetTopKSubgraphsCGQHierarchicalExcludingQueryAndFilterNode
				(K, queryGraph, IMDBGraph, IMDBCGQhierarchicalIndex,
						showProgress, depth, numDFS, numEdgesTouched, 
						fuzzy, avoidTAHeuristics, 
						RAQ.BeamWidth, RAQ.WidthThreshold, doNotUseIndex);
		
		QualitativeHelperHelper (queryGraph, topK, fooWriter_uniform);
		fooWriter_uniform.close();
		*/
		
		//fooWriter.write   (",\n");
		//fooWriter.write   (",\n");
		//fooWriter.write   ("%%%%%%%%%,\n");
		//fooWriter.write   (",\n");
		//fooWriter.write   ("Isomorphism ,\n");
		//fooWriter.write   (",\n");

		//UpdateWeightsUniformly  (queryGraph);
		//QualitativeHelperHelper (K, queryGraph, fooWriter);
		//topK = TraditionalQuery.GetTopSubgraphs(0.25, queryGraph, IMDBGraph, true, true);
		//QualitativeHelperHelper (queryGraph, topK, fooWriter);

	}
	
	private static void QualitativeHelperHelper(Graph<IMDBFeatures> queryGraph,
			ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<IMDBFeatures>.Node>>> topK,
			FileWriter fooWriter) throws IOException {
		//we now have a query graph having m nodes
		//queryGraph.Print();
		//System.out.println("Query Graph ,");
		boolean printExtraComma = true;
		queryGraph.PrintCSVToFile(fooWriter, printExtraComma);
		//printing extra info
		{	
			int index = IMDB.allFeaturesICE.numCommonMovies.ordinalICE();
			if (IMDB.allFeaturesICE.numCommonMovies.isValid()) {
				fooWriter.write(",,weight : "+queryGraph.weights[index]+"\n");
			}
			for (Graph<IMDBFeatures>.Edge qEdge : queryGraph.edgeSet) {
				IMDBFeatures features_1 = qEdge.node1.features;
				IMDBFeatures features_2 = qEdge.node2.features;
				 
				if (IMDB.allFeaturesICE.numCommonMovies.isValid()) {
					int numCommon = (int) (qEdge.GetVe(index)*IMDBFeatures.maxCommonMovies);

					//s="=HYPERLINK(\"https://www.imdb.com/name/"+personID+"\"),";
					fooWriter.write (features_1.name()+
							"("+features_1.personID+")"+
							" -["+numCommon+"]- "+
							features_2.name()+
							"("+features_2.personID+")"+
							",");
					fooWriter.write(",");

					//print the number of common movies
					fooWriter.write("#common movies,"+(numCommon)+",");
				} else {
					fooWriter.write (features_1.name() +" --- "+
							features_2.name()+",");
					fooWriter.write(",");
				}
				fooWriter.write("\n");
			}
		}

		//find the topK
		//System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		//System.out.print("CGQ Hierarchical BFS Index for query graph    :     ");
		//int l=1;
		int r=1;
		for (Helper.ObjectDoublePair<ArrayList<Graph<IMDBFeatures>.Node>> od : topK) {
			ArrayList<Graph<IMDBFeatures>.Node> arr = od.element;
			Double dist = od.value;
			
			//System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			//System.out.println("PRESS ENTER to see the results "+ (l++) +": ");
			//System.in.read();
			
			System.out.println("Mapping");
			for (int j =0 ;j<arr.size(); j++) {
				Graph<IMDBFeatures>.Node qNode = queryGraph.nodeList.get(j);
				Graph<IMDBFeatures>.Node tNode = arr.get(j);

				System.out.print(qNode.features.personID+":"+tNode.features.personID+",");
			}
			System.out.println();
			for (Graph<IMDBFeatures>.Node tNode : arr)
				tNode.features.Print();
			
			/*
			System.out.println(" ***** CSV *****");
			System.out.println(",\nResult : "+ r   +", Distance : "+dist+",");
			*/
			fooWriter.write   (",\nResult : "+ r++ +", Distance : "+dist+",\n");
			
			//print edges
			DecimalFormat dFormat = new DecimalFormat("0.00");
			for (Graph<IMDBFeatures>.Edge qEdge : queryGraph.edgeSet) {
				Graph<IMDBFeatures>.Node qNode1 = qEdge.node1;
				Graph<IMDBFeatures>.Node qNode2 = qEdge.node2;

				int qNode1Index = queryGraph.nodeList.indexOf(qNode1);
				int qNode2Index = queryGraph.nodeList.indexOf(qNode2);
				
				Graph<IMDBFeatures>.Node tNode1 = arr.get(qNode1Index);
				Graph<IMDBFeatures>.Node tNode2 = arr.get(qNode2Index);
				
				Graph<IMDBFeatures>.Edge tEdge = tNode1.GetEdgeTo(tNode2);

				if(tEdge != null) {
					int num = -1;
					if (IMDB.allFeaturesICE.numCommonMovies.isValid()) {
						int index = IMDB.allFeaturesICE.numCommonMovies.ordinalICE();
						num = (int) (tEdge.GetVe(index)*IMDBFeatures.maxCommonMovies);
						
						fooWriter.write (tEdge.node1.features.name() + 
								"("+tEdge.node1.features.personID+")"+
								" -["+num+"]- "+
								tEdge.node2.features.name()+
								"("+tEdge.node2.features.personID+")"+",");
		
					} else {
						fooWriter.write (tEdge.node1.features.name() +" --- "+
								tEdge.node2.features.name()+",");
					}
					//print ve
					for (int i=0; i<tEdge.NumFeatures(); i++)
						fooWriter.write (dFormat.format(tEdge.GetVe(i))+RAQ.Seperator);
					fooWriter.write(",");
					
					//print the number of common movies
					if (IMDB.allFeaturesICE.numCommonMovies.isValid()) {
						fooWriter.write("#common movies,"+num+",");
					}
					//print weight
					/*
				for (int i=0; i<tEdge.NumFeatures(); i++)
					fooWriter.write (dFormat.format(tEdge.GetWeight(i))+RAQ.Seperator);
				fooWriter.write(",");
					 */
				}
				
				fooWriter.write("\n");
			}
			
			int n=0;
			//System.out.print("Node Mapped to,");
			fooWriter.write ("Node Mapped to,");
			
			for (Graph<IMDBFeatures>.Node tNode : arr){
				//tNode.features.PrintCSVHeader();
				tNode.features.PrintCSVHeaderToFile(fooWriter);
				break;
			}
			for (Graph<IMDBFeatures>.Node tNode : arr) {
				//System.out.print(n +   ",");
				fooWriter.write ((n++)+ ":"+tNode.features.name() + ",");
				//tNode.features.PrintCSV();
				tNode.features.PrintCSVToFile(fooWriter);
			}
			fooWriter.write   (",\n");
		}
	}
	
	private static Graph<IMDBFeatures> CreateQueryGraph (Scanner reader ) {
		Graph<IMDBFeatures> graph;
		HashSet<Graph<IMDBFeatures>.Edge> edgesSelected = getQueryGraphEdges(reader);
		if (edgesSelected.size() == 0) {
			graph = new Graph<>(5, false);
		} else {
			ArrayList<Graph<IMDBFeatures>.Edge> edgesArr = new ArrayList<>(edgesSelected);
			graph = IMDBGraph.ConvertEdgesToGraph(edgesArr);
		}
		return graph;
	}

	private static HashSet<Graph<IMDBFeatures>.Edge> getQueryGraphEdges(Scanner reader) {
		class QualitativeComparator 
		implements  Comparator<Graph<IMDBFeatures>.Edge> {

			@Override
			public int compare(Graph<IMDBFeatures>.Edge e1,
					Graph<IMDBFeatures>.Edge e2) {
				String s1 = GetEdgeName (e1, false);
				String s2 = GetEdgeName (e2, false);
				
				return s1.compareTo(s2);
			}
		}
		
		HashSet<Graph<IMDBFeatures>.Node> nodesSelected = new HashSet<>();
		HashSet<Graph<IMDBFeatures>.Edge> edgesSelected = new HashSet<>();

		System.out.println("Creating graph : ");
		
		if (RAQ.test){
			int i =0;
			for (Graph<IMDBFeatures>.Node node : IMDBGraph.nodeList) {
				node.features.Print();
				System.out.println();

				i++;
				if (i>10)
					break;
			}
		}
		//get the first node to start with
		reader.nextLine();
		boolean doNotExit=true;
		
		while (true) {
			System.out.print("NameID : (enter \"exit\" to exit)");
			String name = reader.nextLine();
			globalInput.add(name);
			
			if (name.equals("exit")) {
				doNotExit = false;
				break;
			}
			
			//find the node corresponding to the author
			NameEntry nameEntry = IMDB.nameIDMap.get(name);
			if (nameEntry == null) {
				System.out.println("Invalid name");
				continue;
			}
			
			int nodeID = nameEntry.nodeID;
			//find the node
			Graph<IMDBFeatures>.Node node = IMDBGraph.GetNode (nodeID);
			nodesSelected.add(node);
			
			break;
		}
		if (doNotExit) {
			while (true) {
				int input;
				System.out.print ("exit ? (enter 0 to exit) : ");
				try {
					input = reader.nextInt();
					globalInput.add(""+input);
				} catch (Exception e) {
					reader.next();
					input = 1;
				}

				if (input == 0)
					break;

				//select an edge
				//create an HashSet of possible edges
				HashSet<Graph<IMDBFeatures>.Edge> edges = new HashSet<>();

				for (Graph<IMDBFeatures>.Edge edge : edgesSelected) {
					nodesSelected.add(edge.node1);
					nodesSelected.add(edge.node2);
				}

				for (Graph<IMDBFeatures>.Node node : nodesSelected) {
					edges.addAll(node.AllEdges());
				}

				edges.removeAll(edgesSelected);

				if (edges.isEmpty())
					continue;

				ArrayList<Graph<IMDBFeatures>.Edge> edgesArr = new ArrayList<>(edges);

				Collections.sort(edgesArr, new QualitativeComparator());
				int i=0;
				for (Graph<IMDBFeatures>.Edge edge : edgesArr) {
					System.out.print (i++ + " : ");
					System.out.println(GetEdgeName(edge, false));
				}

				//get the pair of edge connecting 2 previously selected nodes
				ArrayList<ArrayList<Graph<IMDBFeatures>.Edge>> edgesPairArr = getEdgePairsArr(edgesArr, nodesSelected);
				for (ArrayList<Graph<IMDBFeatures>.Edge> arr : edgesPairArr) {
					System.out.print (i++ + " : ");
					Graph<IMDBFeatures>.Edge edge;
					edge = arr.get(0);
					System.out.print (GetEdgeName(edge,false));
					edge = arr.get(1);
					System.out.print (GetEdgeName(edge,true));

					System.out.println();
				}

				System.out.print ("Edge : (-1 for deatiled print, -2 continue without selecting) ");
				try {
					input = reader.nextInt();
					globalInput.add(""+input);
				} catch (Exception e) {
					reader.next();
					continue;
				}

				if (input == -1) {
					for (Graph<IMDBFeatures>.Edge edge : edgesArr) {
						edge.node1.features.PrintCSVHeader();
						break;
					}
					i=0;
					for (Graph<IMDBFeatures>.Edge edge : edgesArr) {
						System.out.print (i++ + " : ");

						PrintEdge(edge);
					}

					System.out.print ("Edge :  ");
					try {
						input = reader.nextInt();
						globalInput.add(""+input);
					} catch (Exception e) {
						reader.next();
						continue;
					}
				} else if (input == -2) {
					//do nothing so that we restart
				} else {
					int n = edgesArr.size();

					if (input < n)
						edgesSelected.add(edgesArr.get(input));
					else {
						input -= n;

						ArrayList<Graph<IMDBFeatures>.Edge> arr = edgesPairArr.get(input);
						edgesSelected.addAll(arr);
					}
				}

				System.out.println("Edges selected : ");
				for (Graph<IMDBFeatures>.Edge edge : edgesSelected) {
					PrintEdge(edge);
				}
			}
		}
		return edgesSelected;
	}

	private static HashSet<Graph<IMDBFeatures>.Edge> getQueryGraphEdgesFromExemplar(Scanner reader) {		
		HashSet<Graph<IMDBFeatures>.Edge> edgesSelected = new HashSet<>();

		System.out.println("Creating graph : ");
		
		//get the first node to start with
		while (true) {
			int input;
			System.out.print ("exit ? (enter 0 to exit) : ");
			try {
				input = reader.nextInt();
			} catch (Exception e) {
				reader.next();
				input = -1;
			}

			if (input == 0)
				break;

			System.out.println("node1 node2 : ");
			int a = reader.nextInt();
			int b = reader.nextInt();
			
			Graph<IMDBFeatures>.Node node1 = IMDBGraph.GetNode(a);
			Graph<IMDBFeatures>.Edge edge  = node1.GetEdgeTo(IMDBGraph.GetNode(b));
			
			edgesSelected.add(edge);
		}
		return edgesSelected;
	}

	/**
	 * get the pair of edge connecting 2 previously selected nodes
	 * @param edgesArr
	 * @param nodesSelected 
	 * @return
	 */
	private static ArrayList<ArrayList<Graph<IMDBFeatures>.Edge>> getEdgePairsArr
	(ArrayList<Graph<IMDBFeatures>.Edge> edgesArr, HashSet<Graph<IMDBFeatures>.Node> nodesSelected) {
		ArrayList<ArrayList<Graph<IMDBFeatures>.Edge>> ret = new ArrayList<>();
		
		int n = edgesArr.size();
		
		for (int i=0; i<n; i++) {
			Graph<IMDBFeatures>.Edge edge_1 = edgesArr.get(i);
			for (int j=i+1; j<n; j++) {
				Graph<IMDBFeatures>.Edge edge_2 = edgesArr.get(j);
				
				//are the edge pair connected
				boolean connected = false;
				if (edge_1.node1 == edge_2.node1) {
					connected = true;
				} else if (edge_1.node1 == edge_2.node2) {
					connected = true;
				} else if (edge_1.node2 == edge_2.node1) {
					connected = true;
				} else if (edge_1.node2 == edge_2.node2) {
					connected = true;
				}
				
				if (connected) {
					HashSet<Graph<IMDBFeatures>.Node> set = new HashSet<>(4);

					set.add(edge_1.node1);
					set.add(edge_1.node2);
					set.add(edge_2.node1);
					set.add(edge_2.node2);
					
					set.removeAll(nodesSelected);
					
					if (set.size() == 1) {
						ArrayList<Graph<IMDBFeatures>.Edge> arr = new ArrayList<>(2);
						arr.add(edge_1);
						arr.add(edge_2);

						ret.add(arr);
					}
				}
			}
		}
		
		return ret;
	}

	private static void PrintEdge(Graph<IMDBFeatures>.Edge edge) {
		Graph<IMDBFeatures>.Node node1, node2;
		String a1 = edge.node1.features.name();
		String a2 = edge.node2.features.name();
		
		if (a1.compareTo(a2) < 0) {
			node1 = edge.node1;
			node2 = edge.node2;
		} else {
			node1 = edge.node2;
			node2 = edge.node1;
		}

		node1.features.Print();
		System.out.print (" --- ");
		node2.features.Print();
		System.out.println();
	}
	/**
	 * Return a name alphabetically sorted
	 * @param e
	 * @return
	 */
	private static String GetEdgeName (Graph<IMDBFeatures>.Edge e, boolean atStart) {
		IMDBFeatures features;
		
		features = e.node1.features;
		String s1 = features.name()+"("+features.importantDetail()+")";
		
		features = e.node2.features;
		String s2 = features.name()+"("+features.importantDetail()+")";
	
		String ret;
		if (s1.compareTo(s2) < 0)
			ret = s1+" --- "+s2;
		else
			ret = s2+" --- "+s1;
		
		//print the number of common movies
		if (IMDB.allFeaturesICE.numCommonMovies.isValid()) {
			int index = IMDB.allFeaturesICE.numCommonMovies.ordinalICE();
			int num = (int) (e.GetVe(index)*IMDBFeatures.maxCommonMovies);
			if(atStart)
				ret = "["+num+"]"+ret+ "\t,["+num+"]";
			else
				ret = ret+ "\t,["+num+"]";
		}
		
		return ret;
	}


	/**
	 * Generate the random inputs
	 * @param setSize : how many random graphs
	 * @param listSize : how many edges in the random graph
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void GenerateRandomGraphs(int setSize, 
			int listSize) throws IOException, InterruptedException, ExecutionException {
		boolean createIndex = true;
		boolean loadQueryGraph=false;
		init(createIndex, loadQueryGraph);
		
		System.out.println("Edges will be printed as list "
				+ "of edges represented as list of node pairs");
		IMDBGraph.GetSetOfRandomListOfConnectedEdges(setSize, listSize);
	}

	/**
	 * Initialize the dataset
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void init() throws IOException, InterruptedException, ExecutionException {
		boolean createIndex = true;
		boolean loadQueryGraph = true;
		init(createIndex, loadQueryGraph);
	}
	/**
	 * Initialize the dataset
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void init (boolean createIndex, boolean loadQueryGraph) throws IOException, InterruptedException, ExecutionException {
		if (IMDBGraph == null) {
			TicToc.Tic();
			
			System.out.println("Loading the IMDB Data set");
			loadData();
			TicToc.Toc();
			probabilityInit();
			CreateGraph ();
			System.out.println("num edges          : "+IMDBGraph.SizeEdges());
			System.out.println("num nodes          : "+IMDBGraph.SizeNodes());
			System.out.println("num non zerornodes : "+IMDBGraph.SizeNodesNonZero());
			printStat();
			TicToc.Toc();
			
			//find the maximum number of common movies
			//printNumCommnonMovies();
			if (createIndex) {
				CreateCGQHierarcicalIndex(RAQ.BranchingFactor);
				TicToc.Toc();
				
				if (loadQueryGraph)
					queryGraphList = IMDBGraph.LoadQueryGraphEdgeList(queryFName);
			}
		}
	}
	
	private static void printStat() throws IOException {
		IMDBStats imdbStats = new IMDBStats();
		collectStat		(imdbStats);
		printimdbStats	(imdbStats);
	}

	
	private static void printimdbStats(IMDBStats imdbStats) throws IOException {
		File myFoo			= new File("../results/QualitativeIMDB/delMeStat.csv");		
		FileWriter fooWriter= new FileWriter(myFoo, false);
		
		fooWriter.write("Median only Rating\n");
		printimdbStatsHelper (imdbStats.medianOlnyRatingCountMap, fooWriter);
		
		fooWriter.write(",\n,\n,\n");
		fooWriter.write("Genre only\n");
		printimdbStatsHelper (imdbStats.genreOnlyCountMap, fooWriter);

		fooWriter.write(",\n,\n,\n");
		fooWriter.write("Primary profession only\n");
		printimdbStatsHelper (imdbStats.primaryProfessionOnlyCountMap, fooWriter);

		fooWriter.write(",\n,\n,\n");
		fooWriter.write("yob only\n");
		printimdbStatsHelper (imdbStats.yobOnlyCountMap, fooWriter);

		fooWriter.write(",\n,\n,\n");
		fooWriter.write("Genre,\n");
		printimdbStatsHelper (imdbStats.genreCountMap, fooWriter);
		
		fooWriter.write(",\n,\n,\n");
		fooWriter.write("MedianRating\n");
		printimdbStatsHelper (imdbStats.medianRatingCountMap, fooWriter);
		
		fooWriter.write(",\n,\n,\n");
		fooWriter.write("MedianRatingActual\n");
		printimdbStatsHelper (imdbStats.medianRatingActualCountMap, fooWriter);
		
		fooWriter.write(",\n,\n,\n");
		fooWriter.write("numCommonMovies\n");
		printimdbStatsHelper (imdbStats.numCommonMoviesCountMap, fooWriter);
		
		fooWriter.write(",\n,\n,\n");
		fooWriter.write("primanr Profession\n");
		printimdbStatsHelper (imdbStats.primaryProfessionCountMap, fooWriter);
		
		fooWriter.write(",\n,\n,\n");
		fooWriter.write("yob\n");
		printimdbStatsHelper (imdbStats.yobCountMap, fooWriter);
	}

	private static void printimdbStatsHelper(HashMap<String, Integer> map, FileWriter fooWriter)
			throws IOException {
		fooWriter.write("id,num,\n");
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			fooWriter.write(entry.getKey()+","+entry.getValue()+"\n");
		}
	}

	/**
	 * parse through all the edges to collect the stats
	 */
	private static void collectStat(IMDBStats imdbStats) {
		for (Graph<IMDBFeatures>.Edge edge : IMDBGraph.edgeSet) {
			Graph<IMDBFeatures>.Node node1 = edge.node1;
			Graph<IMDBFeatures>.Node node2 = edge.node2;

			IMDBFeatures features1 = node1.features;
			IMDBFeatures features2 = node2.features;
			
			String s1,s2;

			s1 = ""+features1.yobActual;
			s2 = ""+features2.yobActual;
			Helper.incrementMap (imdbStats.yobCountMap, 	s1, s2);

			s1 = ""+features1.genreString();
			s2 = ""+features2.genreString();
			Helper.incrementMap (imdbStats.genreCountMap, 	s1, s2);
			
			s1 = ""+features1.medianRating;
			s2 = ""+features2.medianRating;
			Helper.incrementMap (imdbStats.medianRatingCountMap, s1, s2);
			
			s1 = ""+features1.primaryProfessionString();
			s2 = ""+features2.primaryProfessionString();
			Helper.incrementMap (imdbStats.primaryProfessionCountMap, 	s1, s2);

			s1 = ""+features1.medianRatingActual;
			s2 = ""+features2.medianRatingActual;
			Helper.incrementMap (imdbStats.medianRatingActualCountMap, 	s1, s2);

			int index 		= IMDB.allFeaturesICE.numCommonMovies.ordinalICE();
			int numCommon 	= (int) (edge.GetVe(index)*IMDBFeatures.maxCommonMovies);
			Helper.incrementMap (imdbStats.numCommonMoviesCountMap, ""+numCommon);
		}
		
		for (Graph<IMDBFeatures>.Node node : IMDBGraph.nodeList) {
			Helper.incrementMap (imdbStats.yobOnlyCountMap, 				node.features.yobActual+"");
			Helper.incrementMap (imdbStats.genreOnlyCountMap, 				node.features.genreString()+"");
			Helper.incrementMap (imdbStats.medianOlnyRatingCountMap, 		node.features.medianRatingActual+"");
			Helper.incrementMap (imdbStats.primaryProfessionOnlyCountMap, 	node.features.primaryProfessionString()+"");
		}
	}

	public static void Run_Para_vs_Runtime(Experiments.AllExperimentsParameters parameter,
			int mStart, int mEnd, int numQuery,
			ArrayList<Integer> paraL, int maxEdges) throws IOException, InterruptedException, ExecutionException {
		init();

		String  fname=null;
		//useUniformWeight : do we want the query graph to have uniform weight
		boolean useUniformWeight=false;
		
		switch (parameter) {
		case BeamWidth:
			fname=Helper.GetFname(Experiments.AllExperiments.BeamWidth, 1.0, RAQ.DataSet.IMDB.toString());
			break;
		case K_vs_Runtime:
			fname=Helper.GetFname(Experiments.AllExperiments.K_vs_Runtime, 1.0, RAQ.DataSet.IMDB.toString());
			break;
		case K_vs_Runtime_Uniform:
			useUniformWeight = true;
			fname=Helper.GetFname(Experiments.AllExperiments.K_vs_Runtime_uniform, 1.0, RAQ.DataSet.IMDB.toString());
			break;
		case WidthThreshold:
			fname=Helper.GetFname(Experiments.AllExperiments.WidthThreshold, 1.0, RAQ.DataSet.IMDB.toString());
			break;		
		case BranchingFactor:
			System.err.println("Run_Para_vs_Runtime : BranchingFactor Not implemented");
			return;
		}

		ResultLogger.Para_vs_Runtime resultLogger =
				new ResultLogger.Para_vs_Runtime (fname, parameter);

		int m = (mStart+mEnd)/2;
		for (int i=0; i<numQuery; i++) {
			ArrayList<Graph<IMDBFeatures>> queryGraphArray;
			queryGraphArray = IMDBGraph.GetRandomSubgraphArrayFromEdgeList
					(m, m, queryGraphList.get(i));

			Graph<IMDBFeatures> queryGraph =
					queryGraphArray.get(0);

			if (useUniformWeight)
				queryGraph.UpdateWeightsUniformly();
			else
				queryGraph.UpdateWeights(probability);
			System.out.println("Query Graph");
			queryGraph.Print();
			boolean showProgress = false;
			queryGraph.updateNeighbourhoodSummary(showProgress);
			boolean avoidTAHeuristics;

			for (int para : paraL){
				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<IMDBFeatures>.Node>>> topK=null;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				int K = Experiments.K;

				CGQTimeOut.startTimeOut(); 
				TicToc.Tic();
				switch (parameter) {
				case BeamWidth:
					avoidTAHeuristics = !Helper.useHeuristics;
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, IMDBGraph,
							IMDBCGQhierarchicalIndex, true, null, null, null,
							true, avoidTAHeuristics, para, RAQ.WidthThreshold, false);
					break;
				case K_vs_Runtime:
				case K_vs_Runtime_Uniform:
					avoidTAHeuristics = true;
					K = para;
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, IMDBGraph,
							IMDBCGQhierarchicalIndex, true, null, null, null,
							true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
					break;
				case WidthThreshold:
					avoidTAHeuristics = true;
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, IMDBGraph,
							IMDBCGQhierarchicalIndex, true, null, null, null,
							true, avoidTAHeuristics, RAQ.BeamWidth, para, false);
					break;
				case BranchingFactor:
					System.err.println("Run_Para_vs_Runtime : BranchingFactor Not implemented");
					return;
				}

				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();
				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(IMDBGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				int numNodes = queryGraph.SizeNodes();
				int numEdges = queryGraph.SizeEdges();

				RAQ.IndexingScheme indexingScheme = RAQ.IndexingScheme.CGQHierarchicalindexFuzzy;

				resultLogger.Log (numNodes, numEdges, indexingScheme, K, elapsedTime, topK.size(), para);
			}
			resultLogger.Flush();
		}	

	}

	
	@SuppressWarnings("unused")
	private static void printNumCommnonMovies() {
		int max = -1;
		String a=null,b=null;
		
		for (Graph<IMDBFeatures>.Edge edge : IMDBGraph.edgeSet) {
			HashSet<Integer> movies = new HashSet<>(edge.node1.features.movies);
			movies.retainAll(edge.node2.features.movies);
			
			int n = movies.size();
			
			if (n > max) {
				a = edge.node1.features.name();
				b = edge.node2.features.name();
				max = n;
			}
		}
		
		System.out.println("Maximum common movies : "+max);
		System.out.println("edge : "+a+"---"+b);
		System.exit(-1);
	}

	private static void CreateCGQHierarcicalIndex(int branchingFactor) throws InterruptedException, ExecutionException {
		int a, b;
		a=0;
		b=IMDBGraph.nodeList.size();
		System.out.print("Creating CGQHierarchicalindex   :     ");
		ShowProgress.ShowPercent(a, b);
		boolean useHeuristics = false;
		IMDBCGQhierarchicalIndex = new CGQHierarchicalIndex<IMDBFeatures>
		(IMDBGraph.edgeSet,
				IMDBFeatures.numCategoricalFeatures,
				IMDBFeatures.numNonCategoricalFeatures, branchingFactor,useHeuristics);
		
		//for heuristics all edges have to be summarized
		boolean showProgress = true;
		if (useHeuristics)
			IMDBGraph.updateNeighbourhoodSummary(showProgress);

	}

	private static void probabilityInit() {
		int []size =new int[numFeatures];
		Prob = new double[numFeatures][];

		int numFilmfare = 30;
		int numOscar 	= 22;
		int numAward 	= 35;
		do {
			//all this mombo jumbo to ensure we do not miss out a feature
			for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.valuesICE()) {
				int i = feat.ordinalICE();
				int s = -1;
				
				switch (feat) {
				case numFilmFareNominations:
				case numFilmFare:
				case numMoviesFilmFare:
				case numMoviesFilmFareNominations:
					s = numFilmfare;
					break;
				case age:
					s = 150;//assuming no one lives more than 150
					break;
				case genre:
					s = Genre.values().length;
					break;
				case isAlive:
					s=3;
					break;
				case medianRating:
					s = 101; //only 100 possible values
					break;
				case numMovies:
					s = IMDBFeatures.maxMovies()+1;
					break;
				case numTitles:
					s = IMDBFeatures.maxTitles()+1;
					break;
				case professions:
					s = Professions.values().length;
					break;
				case hollywood:
					s = 3;
					break;
				case primaryProfession:
					s = ProfessionCategory.values().length;
					break;
				case numCommonMovies:
					s = (int) IMDBFeatures.maxCommonMovies;
					break;
				case numMoviesOscar:
				case numMoviesOscarNominations:
				case numOscar:
				case numOscarNominations:
					s = numOscar;
					break;
				case yob:
					s = (2018-1750)/IMDBFeatures.ageBucketSize+1;
					break;
				case numAward:
				case numAwardNominations:
				case numMoviesAward:
				case numMoviesAwardNominations:
					s = numAward;
					break;
				}
				
				size[i] = s;
				Prob[i] = new double[s];
			}
		} while (false);

		probability = new Probability(numFeatures, size);
	}

	/**
	 * The graph data structure is being populated
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void CreateGraph() throws IOException, InterruptedException, ExecutionException {
		System.out.println("creating graph");
		//create the graph
		IMDBGraph = new Graph<>(8571176, false);
		//add each name as a node in the graph
		int id=0;

		ProfessionCategory pcList[] = ProfessionCategory.values(); 
				
		for (NameEntry nameEntry : nameIDMap.values()) {
			IMDBFeatures imdbFeature = new IMDBFeatures(nameEntry);
			
			boolean process = true;
			ProfessionCategory pc = pcList[nameEntry.primaryProfession];
			switch (pc) {
			case actor:
			case actress:
			case director:
			case producer:
				break;
			case archive_footage:
			case archive_sound:
			case cinematographer:
			case composer:
			case editor:
			case production_designer:
			case self:
			case writer:
				//discard these
				//FIXME
				process = false;
				break;
			}
			
			if (!process)
				continue;
			
			Graph<IMDBFeatures>.Node node = IMDBGraph.AddNode(id++, imdbFeature);
			nameEntry.addGraphNodeID (node.nodeID);
			//we shall update  the statistic of each feature to
			//find the probability of the feature
			updateProbability (imdbFeature);
		}
		//we now have a count of the features so we shall now make it a probability
		updateProbability (null);
		
		System.out.println("adding edges");
		//add edges
		for (TitleEntry titleEntry : titleIDMap.values()) {
			//put edge between all people associated with this title
			//we are only considering movies
			if (titleEntry.isMovie()) {
				for (String name1 : titleEntry.names) {
					for (String name2 : titleEntry.names) {
						if (name1.compareTo(name2) > 0) {
							//ensures that we add only once
							NameEntry entry = nameIDMap.get(name1);
							if (entry == null)
								continue;
							
							int source 		= entry.nodeID;
							
							entry = nameIDMap.get(name2);
							if (entry == null)
								continue;
							int destination = entry.nodeID;

							IMDBGraph.AddEdge(source, destination);
						}
					}	
				}
			}
		}
		
		//we shall update  the statistic of each feature to
		//find the probability of the feature
		probability.UpdateProbability (IMDBGraph.edgeSet);
		
		//we now have a count of the features so we shall now make it a probability
		probability.UpdateProbability ();

	}

	private static void updateProbability(IMDBFeatures feature) {
		if (feature == null) {
			//we have to convert all the counts in to probability
			for (int f=0; f<Prob.length; f++) {
				double sum = 0.0;
				for (int d=0; d<Prob[f].length; d++) {
					sum += Prob[f][d];
				}
				for (int d=0; d<Prob[f].length; d++) {
					Prob[f][d] /= sum;
				}
			}
		} else {
			do {
				//all this mombo jumbo to ensure we do not miss out a feature
				for (IMDB.allFeaturesICE feat : IMDB.allFeaturesICE.valuesICE()) {
					int i = feat.ordinalICE();
					switch (feat) {
					case age:
						Prob[i][feature.age]++;
						break;
					case genre:
						for (Integer g : feature.genre)
							Prob[i][g]++;
						break;
					case isAlive:
						Prob[i][feature.isAlive]++;
						break;
					case medianRating:
						Prob[i][feature.medianRating]++;
						break;
					case numMovies:
						Prob[i][feature.numMovies]++;
						break;
					case numTitles:
						Prob[i][feature.numTitles]++;
						break;
					case professions:
						for (Integer p : feature.professions)
							Prob[i][p]++;
						break;
					case hollywood:
						Prob[i][feature.isHollywood]++;
						break;
					case primaryProfession:
						Prob[i][feature.primaryProfession]++;
						break;
					case numCommonMovies:
						//nothing to be done
						//In IMDB we are not making use of Prob so it is fine to ignore
						break;
					case numMoviesOscar:
						Prob[i][feature.numMoviesOscar]++;
						break;
					case numMoviesOscarNominations:
						Prob[i][feature.numMoviesOscarNominations]++;
						break;
					case numOscar:
						Prob[i][feature.numOscar]++;
						break;
					case numOscarNominations:
						Prob[i][feature.numOscarNominations]++;
						break;
					case yob:
						Prob[i][feature.yob]++;
						break;
					case numMoviesFilmFare:
						Prob[i][feature.numMoviesFilmFare]++;
						break;
					case numMoviesFilmFareNominations:
						Prob[i][feature.numMoviesFilmFareNominations]++;
						break;
					case numFilmFare:
						Prob[i][feature.numFilmFare]++;
						break;
					case numFilmFareNominations:
						Prob[i][feature.numFilmFareNominations]++;
						break;
					case numMoviesAward:
						Prob[i][feature.numMoviesAward]++;
						break;
					case numMoviesAwardNominations:
						Prob[i][feature.numMoviesAwardNominations]++;
						break;
					case numAward:
						Prob[i][feature.numAward]++;
						break;
					case numAwardNominations:
						Prob[i][feature.numAwardNominations]++;
						break;
					}
				}
			}while (false);
		}
	}

	private static void loadData() throws IOException {
		//loadAward();
		loadName();
		loadTitle();
		//for each person populate the title associated with him
		updateNameEntries ();
	}

	/**
	 * Load the list of all nominees and winners
	 * @throws IOException 
	 */
	private static void loadAward() throws IOException {
		loadOscar();
		loadFilmFare();
	}

	private static void loadFilmFare() throws FileNotFoundException, IOException {
		String fName = "../data/imdb/filmfare/";
		maxNomination = 0;
		loadAwardPeople(fName, filmFareNominationsMap, filmFareWinnersMap);
		loadAwardMovies(fName, filmFareNominationsMap, filmFareWinnersMap, 
				filmFareNominationsMovies, filmFareWinnersMovies);
	}

	private static void loadOscar() throws FileNotFoundException, IOException {
		String fName = "../data/imdb/oscar/";
		maxNomination = 0;
		loadAwardPeople(fName, oscarNominationsMap, oscarWinnersMap);
		loadAwardMovies(fName, oscarNominationsMap, oscarWinnersMap, 
				oscarNominationsMovies, oscarWinnersMovies);
	}

	private static void loadAwardMovies(String fName, HashMap<String,Integer> nominationsMap,
			HashMap<String,Integer> winnersMap,
			HashSet<String> nominationsMovies,
			HashSet<String> winnersMovies) throws IOException {
		loadAwardMovies(fName, true,  nominationsMap, winnersMap, 
				nominationsMovies, winnersMovies);
		loadAwardMovies(fName, false, nominationsMap, winnersMap, 
				nominationsMovies, winnersMovies);
	}

	private static void loadAwardMovies(String fName, boolean isNomiation,
			HashMap<String,Integer> nominationsMap,
			HashMap<String,Integer> winnersMap,
			HashSet<String> nominationsMovies,
			HashSet<String> winnersMovies) throws IOException {
		HashSet<String> set;
		fName += "pictures";
		
		if (isNomiation) {
			fName += "Nominations.txt";
			set = nominationsMovies;
		} else {
			fName += "Winners.txt";
			set = winnersMovies;
		}
		
		//we are only interested in movies
		BufferedReader br = new BufferedReader(new FileReader(fName));

		String l;
		//ignore the first line
		while ((l = br.readLine()) != null) {
			set.add (l);
		}

		br.close();
	}

	private static void loadAwardPeople(String fName, HashMap<String,Integer> nominationsMap,
			HashMap<String,Integer> winnersMap) throws FileNotFoundException, IOException {
		loadAwardPeopleHelper(fName+"actor", 	nominationsMap, winnersMap);
		loadAwardPeopleHelper(fName+"director", nominationsMap, winnersMap);
		loadAwardPeopleHelper(fName+"producers",nominationsMap, winnersMap);
		System.out.println("maxNomination "+maxNomination);
	}

	private static void loadAwardPeopleHelper(String fName, HashMap<String,Integer> nominationsMap,
			HashMap<String,Integer> winnersMap) throws FileNotFoundException, IOException {
		loadAwardPeopleHelperHelper(fName, true,  nominationsMap, winnersMap);
		loadAwardPeopleHelperHelper(fName, false, nominationsMap, winnersMap);
	}

	private static void loadAwardPeopleHelperHelper(String fName, boolean isNomination,
			HashMap<String,Integer> nominationsMap,
			HashMap<String,Integer> winnersMap)
			throws FileNotFoundException, IOException {
		String l;
		HashMap<String, Integer> map;
		
		if (isNomination) {
			fName += "Nominations.txt";
			map = nominationsMap;
		} else {
			fName += "Winners.txt";
			map = winnersMap;
		}
		
		//we are only interested in movies
		BufferedReader br = new BufferedReader(new FileReader(fName));
		
		//ignore the first line
		while ((l = br.readLine()) != null) {
			Integer count = map.get(l);
			
			if (count == null)
				count = 0;
			
			count++;
			
			if (count > maxNomination)
				maxNomination = count;
			map.put(l, count);
		}
		
		br.close();
		
	}

	/**
	 * for each person populate the title associated with him
	 * @throws IOException 
	 */
	private static void updateNameEntries() throws IOException {
		processPrincipals();
		//processCrew();
		
		//update the mean ratings
		updateNameEntryRatings();
	}

	/**
	 * Find the average rating of the person
	 */
	private static void updateNameEntryRatings() {
		for (NameEntry entry : nameIDMap.values()) {
			//int rating = 0;
			//int n=0;
			
			ArrayList<Integer> ratings 		= new ArrayList<>();
			ArrayList<Integer> ratingsmovie = new ArrayList<>();

			int numHollywood=0;
			int numBollywood=0;
			
			/*
			//number of time a profession was appraised
			int numDirector=0;
			int numWriter=0;
			int numActor=0;
			 */
			
			ArrayList<Integer> genreList 		= new ArrayList<>();
			ArrayList<Integer> genreListMovie 	= new ArrayList<>();
			
			for (int title : entry.titles) {
				TitleEntry tEntry = titleIDMap.get(title);
				if (tEntry == null)
					continue;				
				
				switch (Region.BollyWood) {
				case BollyWood:
					if (tEntry.region == Region.BollyWood)
						numBollywood++;
				case HollyWood:
					if (tEntry.region == Region.HollyWood)
						numHollywood++;
				}
				
				int r = tEntry.rating();
				if (r>0)
					ratings.add(r);
				
				genreList.addAll(tEntry.genres);
				if (tEntry.isMovie()) {
					if (r>0)
						ratingsmovie.add(r);
					genreListMovie.addAll(tEntry.genres);
				}
				
				//rating += tEntry.rating();
				//n++;
			}
			
			//if (n > 0)
				//rating /= n;
			int medianRating 		= 0;
			int medianRatingMovies 	= 0;
			if (ratings.size() > 0) {
				Collections.sort(ratings);
				if (ratings.isEmpty())
					medianRating = 0;
				else
					medianRating = ratings.get(ratings.size()/2);
				
				if (ratingsmovie.size() > 0) {
					Collections.sort(ratingsmovie);
					if(ratingsmovie.isEmpty())
						medianRatingMovies = 0;
					else
						medianRatingMovies = ratingsmovie.get(ratingsmovie.size()/2);
				}
				//get the median rating
			}
			entry.updateMedianRating(medianRating, medianRatingMovies);

			HashSet<Integer> topKGenre		= getTop3Genre (genreList, 		genreK);
			HashSet<Integer> topKGenreMovie = getTop3Genre (genreListMovie, genreK);
			
			entry.updateGenre  (topKGenre, topKGenreMovie);
			entry.updateRegion (numHollywood, numBollywood);
			
			//update the primary profession
			/*
			ProfessionCategory p=null;
			if (numActor > numDirector) {
				if (numActor > numWriter) {
					 p = ProfessionCategory.actor;
				} else {
					 p = PrimaryProfession.writer;
				}
			} else {
				if (numDirector > numWriter) {
					 p = PrimaryProfession.director;
				} else {
					p = PrimaryProfession.writer;
				}
			}
			entry.updatePrimaryProfession (p);
			*/
			
			//find the one most important profession
			{
				int count[] = new int[ProfessionCategory.values().length];
				
				for (ProfessionCategory pc : entry.professionCategoryList) {				
					//discard self and archive_footage
					boolean process = true;
					
					switch (pc) {
					case actor:
					case actress:
					case cinematographer:
					case composer:
					case director:
					case editor:
					case producer:
					case writer:
						break;
					case production_designer:
					case archive_footage:
					case archive_sound:
					case self:
						process = false;
						break;					
					}
					
					if (process) {
						int i = pc.ordinal();
						count[i]++;
					}
				}
				//free memory
				entry.professionCategoryList = null;
				
				//find the max
				int max = -1;
				int index = 0;
				
				for (int i=0; i<count.length; i++) {
					if (count[i]>max) {
						max = count[i];
						index = i;
					}
				}
				
				if(entry.id.compareTo("nm0000138") == 0)
					//leanardo di caprio is primarily a producer, we want to make him an actor
					index = ProfessionCategory.actor.ordinal();

				//we are treating Actress as Actor
				if (IMDB.allFeaturesICE.treatActressAsActor)
					if (index == ProfessionCategory.actress.ordinal())
						index = ProfessionCategory.actor.ordinal();
				
				entry.updatePrimaryProfession(index);
			}
			
			//update the maxTitles
			int numMovies = entry.movies.size();
			int numTitles = entry.titles.size();
			
			IMDBFeatures.updateMaxTitles (numTitles, numMovies);
		}
	}

	private static HashSet<Integer> getTop3Genre(ArrayList<Integer> genreList, int k) {
		/**
		 //Java 8 code
 		Map<Integer, Long> map = genreList.stream().collect(Collectors.groupingBy(w -> w, Collectors.counting()));
 		List<Entry<Integer, Long>> result = map.entrySet().stream()
 				.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
 				.limit(3)
 				.collect(Collectors.toList());
 		
 		HashSet<Integer> ret = new HashSet<>(3);
 		for (Entry<Integer, Long> e : result)
 			ret.add(e.getKey());
 		return ret;
		 */
		HashMap<Integer, Integer> map = new HashMap<>();
		for (Integer s : genreList) {
			Integer count = map.get(s);
			if (count == null) 
				count = 0;
			count++;
			map.put(s, count);
		}

		ArrayList<Map.Entry<Integer, Integer>> entryList = new ArrayList<>(map.entrySet());
		
		class EntryComparator implements Comparator<Map.Entry<Integer, Integer>> {
			@Override
			public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
				return -1*Integer.compare(o1.getValue(), o2.getValue());
			}			
		}
		EntryComparator ec = new EntryComparator();
		
		entryList.sort(ec);
		
		HashSet<Integer> ret = new HashSet<>(k);
		int i=0;
 		for (Entry<Integer, Integer> e : entryList) {
 			ret.add(e.getKey());
 			i++;
 			if (i==k)
 				break;
 		}
 			
 		return ret;
	}

	private static void processRatings() throws IOException {
		String fName="../data/imdb/title_ratings.tsv";
		String l;
		
		//we are only interested in movies
		BufferedReader br = new BufferedReader(new FileReader(fName));
		
		//ignore the first line
		l = br.readLine();
		while ((l = br.readLine()) != null) {
			String s[] = l.split("\\t");
			
			int titleID = Integer.parseInt(s[0].substring(2));
			TitleEntry entry = titleIDMap.get(titleID);
			if (entry == null)
				continue;
			double r = Double.parseDouble(s[1]);
			
			int rating = (int) (r*10); //we donot need a float
			
			entry.updateRating(rating);
		}
		
		br.close();		
	}

	/*
	private static void processCrew() throws IOException {
		String fName="../data/imdb/title_crew.tsv";
		String l;
		
		//we are only interested in movies
		BufferedReader br = new BufferedReader(new FileReader(fName));
		
		//ignore the first line
		l = br.readLine();
		while ((l = br.readLine()) != null) {
			String s[] = l.split("\\t");
			int titleID = Integer.parseInt(s[0].substring(2));
			TitleEntry tEntry = titleIDMap.get(titleID);
			if (tEntry == null)
				continue;
			
			for (int i=1; i<3; i++) {
				if (s[i].compareTo("\\N") != 0) {
					String t[] = s[i].split(",");
					for (String e : t) {
						String nameID = e;
						//add the title to the name entry
						NameEntry entry = nameIDMap.get(nameID);
						if (entry != null) {
							entry.addTitle(titleID, tEntry.isMovie());
							if (i == 1)
								tEntry.addPerson (nameID, PrimaryProfession.director);
							else
								tEntry.addPerson (nameID, PrimaryProfession.writer);
						}
					}
				}
			}
		}
		
		br.close();
	}
	*/

	/**
	 * load the principals
	 * @throws IOException 
	 */
	private static void processPrincipals() throws IOException {
		String fName="../data/imdb/title_principals.tsv";
		String l;
		
		//we are only interested in movies
		BufferedReader br = new BufferedReader(new FileReader(fName));
		
		//ignore the first line
		l = br.readLine();
		while ((l = br.readLine()) != null) {
			String s[] = l.split("\\t");
			String title 	= s[0];
			int titleID 	= Integer.parseInt(s[0].substring(2));
			String nameID  	= s[2];
			String category = s[3].replaceAll(" ", "_");
			
			//add the title to the name entry
			NameEntry  entry 	= nameIDMap .get(nameID);
			TitleEntry tEntry 	= titleIDMap.get(titleID);
			
			if (entry != null && tEntry != null) {
				entry.addTitle(titleID, tEntry.isMovie(), 
						ProfessionCategory.valueOf(category), title);
				tEntry.addPerson (nameID);
			}
		}
		
		br.close();
		System.out.println("max movie nominations : "+maxNominationMovies);
	}

	/**
	 * Load the information about a title
	 * @throws IOException 
	 */
	private static void loadTitle() throws IOException {
		//getting valid titles
		HashMap<Region, HashSet<Integer>> validIDMap = getValidTitleIDs ();
		String fName="../data/imdb/title_basics.tsv";
		String l;

		HashSet<Integer> bollywood = validIDMap.get(Region.BollyWood);
		HashSet<Integer> hollywood = validIDMap.get(Region.HollyWood);
		
		//we are only interested in movies
		BufferedReader br = new BufferedReader(new FileReader(fName));
		
		//ignore the first line
		l = br.readLine();
		while ((l = br.readLine()) != null) {
			String s[] = l.split("\\t");
			
			//tconst	titleType	primaryTitle	originalTitle	isAdult	startYear	endYear	runtimeMinutes	genres
			int i=0;
			int id = Integer.parseInt(s[i].substring(2));
			
			Region region = null;
			
			boolean process = false;
			
			for (Region r : Region.values()) {
				if (r.toBeConsidered()) {
					switch (r) {
					case BollyWood:
						if (bollywood.contains(id)) {
							process = true;
							region 	= r;
						}
						break;
					case HollyWood:
						if (hollywood.contains(id)) {
							process = true;
							region 	= r;
						}
						break;
					}
				}
			}
			
			if (!process)
				continue;
			i++;
			if (s[i].compareTo("short") == 0)
				s[i]="short_type";
			
			if (!TitleTypes.valueOf(s[i]).toBeConsidered())
				continue;
			
			int titleType = TitleTypes.indexof(s[i]);
			i++;
			String primaryTitle = s[i];
			i++;
			String originalTitle = s[i];
			i++;
			boolean isAdult = Integer.parseInt(s[i])>0;
			int startYear;
			i++;
			if (s[i].compareTo("\\N") == 0)
				startYear = -1;
			else
				startYear = Integer.parseInt(s[i]);
			int endYear;
			i++;
			if (s[i].compareTo("\\N") == 0)
				endYear = -1;
			else
				endYear = Integer.parseInt(s[i]);
			
			int runtimeMinutes;
			i++;
			if (s[i].compareTo("\\N") == 0)
				runtimeMinutes =  -1;
			else 
				runtimeMinutes = Integer.parseInt(s[i]);
			
			HashSet<Integer> genres = new HashSet<>();
			i++;			
			if (s[i].compareTo("\\N") != 0) {
				String t[] = s[i].split(",");
				for (String g : t) {
					g = g.replaceAll("-", "_");

					genres.add(Genre.indexof(g));
				}
			}
			
			TitleEntry entry = new TitleEntry(id, titleType, primaryTitle,
					originalTitle, isAdult,startYear, endYear,
					runtimeMinutes, genres, region);
			titleIDMap.put(id, entry);
		}
		
		
		br.close();
		
		processRatings();
	}

	/**
	 * We are only interested in titles from US GB and IN
	 * Map value 
	 * @return
	 * @throws IOException 
	 */
	private static HashMap<Region, HashSet<Integer>> getValidTitleIDs() throws IOException {
		String fName="../data/imdb/title_akas.tsv";
		String l;
		
		HashMap<Region, HashSet<Integer>> validID = new HashMap<>(Region.values().length);

		HashSet<Integer> hollywood = new HashSet<>();
		HashSet<Integer> bollywood = new HashSet<>();
		
		//we are only interested in movies
		BufferedReader br = new BufferedReader(new FileReader(fName));
		
		//ignore the first line
		l = br.readLine();
		while ((l = br.readLine()) != null) {
			String s[] = l.split("\\t");
			switch (s[3]) {
			case "GB":
			case "US":
				int id = Integer.parseInt(s[0].substring(2));
				hollywood.add(id);
				break;
			case "IN":
				id = Integer.parseInt(s[0].substring(2));
				bollywood.add(id);	
				break;
			default:
				break;
			}
		}
		br.close();
		
		for (Region region : Region.values()) {
			if (region.toBeConsidered()) {
				switch (region) {
				case BollyWood:
					validID.put(Region.BollyWood, bollywood);
					break;
				case HollyWood:
					validID.put(Region.HollyWood, hollywood);
					break;
				}
			}
		}
		
		return validID;
	}

	/**
	 * load information about the people
	 * @throws IOException
	 */
	private static void loadName() throws IOException {
		HashSet<String> nonBollywood = loadNonBollywood ();
		String fName="../data/imdb/name_basics.tsv";
		String l;
		
		//load the data

		//make a list of profession
		int minYob = 2018;
		BufferedReader br = new BufferedReader(new FileReader(fName));
		
		//ignore the first line
		l = br.readLine();
		while ((l = br.readLine()) != null) {
			String s[] = l.split("\\t");
			
			String id;
			String primaryName;
			int birthYear,deathYear; //-1 if no value available
			HashSet<Integer> primaryProfession=new HashSet<>();
			HashSet<Integer> knownForTitles=new HashSet<>();

			//get the id
			int i=0;
			id = s[i];

			//avoid non bollywood people
			if (nonBollywood.contains(id)) {
				continue;
			}
			
			i++;
			primaryName = s[i];

			i++;
			if (s[i].compareTo("\\N") == 0)
				birthYear = -1;
			else
				birthYear = Integer.parseInt(s[i]);
			
			if (birthYear <1800)
				birthYear = -1;

			//discard the node of birth year information not available
			if (birthYear < 0)
				continue;
			
			//We are discarding older people
			//FIXME
			//if (birthYear < 1948)
			//if (birthYear < 1935)
			//if (birthYear < 1900)
				//continue;
			
			if (birthYear >=0)
				if (birthYear < minYob)
					minYob = birthYear;
			
			i++;
			if (s[i].compareTo("\\N") == 0)
				deathYear = -1;
			else
				deathYear = Integer.parseInt(s[i]);

			i++;
			if (s[i].compareTo("\\N") != 0) {
				String profession[] = s[i].split(",");

				for (String p : profession) {
					if (p.length() > 0)
						primaryProfession.add(Professions.indexof(p));
				}
			}

			i++;
			if (s[i].compareTo("\\N") != 0) {
				String titles[] = s[i].split(",");

				for (String t : titles) {
					String t1 = t.substring(2);

					knownForTitles.add(Integer.parseInt(t1));
				}
			}
			
			int numOscar 				= oscarWinnersMap.getOrDefault		 (id, 0);
			int numOscarNominations		= oscarNominationsMap.getOrDefault	 (id, 0);
			
			int numFilmFare				= filmFareWinnersMap.getOrDefault	 (id, 0);
			int numFilmFareNominations	= filmFareNominationsMap.getOrDefault(id, 0);
			
			NameEntry entry = new NameEntry(id, primaryName, birthYear,
					deathYear, primaryProfession, knownForTitles,
					numOscar, 	numOscarNominations,
					numFilmFare,numFilmFareNominations);
			nameIDMap.put(id, entry);
		}
		
		System.out.println("Min yob : "+minYob);
		br.close();
	}

	/**
	 * List of Indian personalities not in bollywood
	 * @return
	 * @throws IOException 
	 */
	private static HashSet<String> loadNonBollywood() throws IOException {
		String fName="../data/imdb/name_NonBollywood.txt";
		String l;
		
		HashSet<String> ret = new HashSet<>();
		//load the data

		//make a list of profession
		BufferedReader br = new BufferedReader(new FileReader(fName));
		
		//ignore the first line
		l = br.readLine();
		while ((l = br.readLine()) != null) {
			if (l.length() > 0)
				ret.add(l);
		}
		
		br.close();
		
		return ret;
	}


	public static void PrintResults (Experiments.AllExperiments experiment) {
		boolean minimal = false;

		PrintResults (experiment, minimal);
	}

	public static void PrintResults (Experiments.AllExperiments experiment, boolean minimal) {
		String fname=Helper.GetFname(experiment, 1.0, RAQ.DataSet.IMDB.toString());
		switch (experiment) {
		case BaseCase: {
			ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,
					RAQ.IndexingScheme.BaseCase);
			resultLogger.PrintAllResults ();
		}
		break;
		case BeamWOIndex: {
			ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,
					RAQ.IndexingScheme.BFSWOIndex);
			resultLogger.PrintAllResults ();
		}
		break;
		case Heuristicts: {
			ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,
					RAQ.IndexingScheme.BFSWOHeuristics);
			resultLogger.PrintAllResults ();
		}
		break;
		case BeamWidth: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.BeamWidth);
			resultLogger.PrintAllResults ();
		}
		break;
		case BranchingFactor: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.BranchingFactor);
			resultLogger.PrintAllResults ();
		}
		break;
		case Exit:
			break;
		case Graphsize_vs_Runtime:
		case Graphsize_vs_Runtime_uniform: {
			ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);			
			resultLogger.PrintAllResults (-1, minimal);
		}
		break;
		case K_vs_Runtime: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.K_vs_Runtime);
			resultLogger.PrintAllResults (minimal);
		}
		break;
		case K_vs_Runtime_uniform: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.K_vs_Runtime_Uniform);
			resultLogger.PrintAllResults (minimal);
		}
		break;
		case Qualitative:
			break;
		case QualitativeQuantitative:
			break;
		case TargetGraphSize: {
			double frac = Experiments.GetFraction();
			fname=Helper.GetFname(experiment, frac, RAQ.DataSet.IMDB.toString());
			ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);			
			resultLogger.PrintAllResults (-1, minimal);
		}
		break;
		case WidthThreshold:
			break;
		}
	}

	
	public static void Run_Graphsize_vs_Runtime_withUniformWeight(int mStart, int mEnd,
			int numQuery, int K, int maxEdges) throws IOException, InterruptedException, ExecutionException {
		boolean printResults = true;
		boolean considerUniformWeight = true;
		Run_Graphsize_vs_Runtime(mStart, mEnd, numQuery, K, maxEdges, printResults, considerUniformWeight);
	}

	public static void Run_Graphsize_vs_Runtime(int mStart, int mEnd,
			int numQuery, int K, int maxEdges) throws IOException, InterruptedException, ExecutionException {
		boolean printResults = true;
		boolean considerUniformWeight = false;
		Run_Graphsize_vs_Runtime(mStart, mEnd, numQuery, K, maxEdges, printResults, considerUniformWeight);
	}

	public static void Run_Graphsize_vs_Runtime(int mStart, int mEnd,
			int numQuery, int K, int maxEdges, boolean printResults,
			boolean considerUniformWeights) throws IOException, InterruptedException, ExecutionException {
		init();
		
		String fname=Helper.GetFname(Experiments.AllExperiments.Graphsize_vs_Runtime, 1.0, RAQ.DataSet.IMDB.toString());
		ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);
		//create Query query Graphs
		ArrayList<Graph<IMDBFeatures>> queryGraphArray;
		for (int i=0; i<numQuery; i++) {
			queryGraphArray = IMDBGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<IMDBFeatures> queryGraph : queryGraphArray) {
				//we do not want to run an experiment on more than max edges 
				if(queryGraph.SizeEdges() > maxEdges)
					break;

				//we now have a query graph having m nodes
				if (considerUniformWeights) 
					queryGraph.UpdateWeightsUniformly();
				else
					queryGraph.UpdateWeights(probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<IMDBFeatures>.Node>>> topK;

				boolean avoidTAHeuristics = !Helper.useHeuristics;
				if (printResults)
					queryGraph.Print();
				showProgress = printResults;
				System.out.print("Processing query graph "+i+" of size "+queryGraph.SizeEdges() +"    :     ");
				CGQTimeOut.startTimeOut(); 
				TicToc.Tic(printResults);
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, IMDBGraph,
						IMDBCGQhierarchicalIndex, showProgress, null, null, null,
						true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();
				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(IMDBGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				int numNodes = queryGraph.SizeNodes();
				int numEdges = queryGraph.SizeEdges();

				RAQ.IndexingScheme indexingScheme = RAQ.IndexingScheme.CGQHierarchicalindexFuzzy;

				resultLogger.Log (numNodes, numEdges, indexingScheme, K, elapsedTime, topK.size(), -1);
			}

			resultLogger.Flush();
		}

	}
	
	/**
	 * Generate all the files as required by Exemplar
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void RunExemplar() throws IOException, InterruptedException, ExecutionException {
		boolean creatIndex = false;
		boolean loadQuery = false;
		init(creatIndex, loadQuery);
		
		String baseName="../results/IMDB/";
		
		runExemplarSin(baseName);
		
	}

	/**
	 * Generate all the files as required by Exemplar
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void RunExemplarQuery(Scanner reader) throws IOException, InterruptedException, ExecutionException {
		boolean creatIndex = false;
		boolean loadQuery = false;
		init(creatIndex, loadQuery);
		
		HashSet<Graph<IMDBFeatures>.Edge> edgesSelected = getQueryGraphEdges(reader);
		
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		//convert the edges into query file
		for (Graph<IMDBFeatures>.Edge edge : edgesSelected) {
			int sourceID 		= edge.node1.nodeID;
			int destinationID 	= edge.node2.nodeID;
			
			//find the label
			String l = null;

			IMDBFeatures f1 = edge.node1.features;
			IMDBFeatures f2 = edge.node2.features;
			
			for (allFeaturesICE f : allFeaturesICE.validValues) {
				l = getLabel(f1, f2, f);
				
				//adding the edge
				System.out.println(sourceID+" "+destinationID+" "+l);
				System.out.println(destinationID+" "+sourceID+" "+l);
			}
		}
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
	}

	/**
	 * Generate all the files as required by Exemplar
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void RunExemplarQueryReverse(Scanner reader) throws IOException, InterruptedException, ExecutionException {
		boolean creatIndex = false;
		boolean loadQuery = false;
		init(creatIndex, loadQuery);
		
		HashSet<Graph<IMDBFeatures>.Edge> edgesSelected = getQueryGraphEdgesFromExemplar(reader);
		
		Graph<IMDBFeatures> graph;
		ArrayList<Graph<IMDBFeatures>.Edge> edgesArr = new ArrayList<>(edgesSelected);
		graph = IMDBGraph.ConvertEdgesToGraph(edgesArr);
		
		graph.PrintCSV();
	}

	/**
	 * generate the sin and sout files 
	 * in this case it only one file
	 * also generate the label frequencies
	 * @param baseName
	 * @throws IOException
	 */
	private static void runExemplarSin(String baseName) throws IOException {
		//generate the sin and sout files
		//in this case it only one file
		File myFoo = new File(baseName+"IMDB-sin.graph");
		FileWriter fooWriter = new FileWriter(myFoo, false);
		
		HashMap<String, Integer> labelFrequencyMap = new HashMap<>();
		
		for (Graph<IMDBFeatures>.Edge edge : IMDBGraph.edgeSet) {
			int sourceID 		= edge.node1.nodeID;
			int destinationID 	= edge.node2.nodeID;
			
			//find the label
			String l = null;

			IMDBFeatures f1 = edge.node1.features;
			IMDBFeatures f2 = edge.node2.features;
			
			for (allFeaturesICE f : allFeaturesICE.validValues) {
				l = getLabel(f1, f2, f);
				
				//adding the edge
				fooWriter.write(sourceID     +" "+destinationID+" "+l+"\n");
				fooWriter.write(destinationID+" "+sourceID     +" "+l+"\n");
				
				Integer count = labelFrequencyMap.get(l);
				if (count == null)
					count = 0;
				count++;
				labelFrequencyMap.put(l, count);
			}
		}

		fooWriter.close();

		//save the label frequencies
		myFoo = new File(baseName+"IMDB-label-frequencies.csv");
		fooWriter = new FileWriter(myFoo, false);

		for (Entry<String, Integer> entry : labelFrequencyMap.entrySet()) {
			fooWriter.write(entry.getKey()+" "+entry.getValue()+"\n");
		}
		fooWriter.close();
	}

	private static String getLabel(IMDBFeatures f1, IMDBFeatures f2, allFeaturesICE f) {
		String l=null;
		switch (f) {
		case hollywood:
			l="1"+getLabelCategoricalHelper(f1.isHollywood, f2.isHollywood);
			break;
		case isAlive:
			l="2"+getLabelCategoricalHelper(f1.isAlive, f2.isAlive);
			break;
		case primaryProfession:
			l="5"+getLabelCategoricalHelper(f1.primaryProfession, f2.primaryProfession);
			break;
		case medianRating:					
			l="3"+runExemplar_Helper(f1.medianRating, f2.medianRating);
			break;
		case numCommonMovies:
			HashSet<Integer> m = new HashSet<>(f1.movies);
			m.retainAll(f2.movies);
			
			int s = m.size()/5;
			if (s > 10)
				s = 10;
			l="4"+s;
			break;
		case yob:
			l="6"+runExemplar_Helper(f1.yob, f2.yob);
			break;
		case genre:
			int g1=0;
			for (int g: f1.genre) {
				g1=g;
				break;
			}
			int g2=0;
			for (int g: f2.genre) {
				g2=g;
				break;
			}
			l="7"+getLabelCategoricalHelper(g1, g2);
			break;
			/////////////////////////
		case age:
			break;
		case numAward:
			break;
		case numAwardNominations:
			break;
		case numFilmFare:
			break;
		case numFilmFareNominations:
			break;
		case numMovies:
			break;
		case numMoviesAward:
			break;
		case numMoviesAwardNominations:
			break;
		case numMoviesFilmFare:
			break;
		case numMoviesFilmFareNominations:
			break;
		case numMoviesOscar:
			break;
		case numMoviesOscarNominations:
			break;
		case numOscar:
			break;
		case numOscarNominations:
			break;
		case numTitles:
			break;
		case professions:
			break;
		}
		return l;
	}
	
	private static String getLabelCategoricalHelper(int a, int b) {
		if (a == b)
			return ("1");
		else
			return ("0");
	}

	static String runExemplar_Helper (int a , int b) {
		String s="000";
		
		if (a < b)
			s = a+s+b;
		else
			s = b+s+a;
		
		return s;
	}
}

class ToBeConsidered {
	static boolean hollywood = true;
	static boolean bollywood = true;
}

enum Region {
	//FIXME
	//HollyWood(false),
	//HollyWood(ToBeConsidered.hollywood),
	//BollyWood(ToBeConsidered.bollywood);
	HollyWood(true),
	BollyWood(true);
	
	private boolean toBeConsidered;
	
	Region (boolean b){
		toBeConsidered = b;
	}
	
	public void donotConsider() {
		switch (this) {
		case BollyWood:
			ToBeConsidered.bollywood = false;
			break;
		case HollyWood:
			ToBeConsidered.hollywood = false;
			break;
		}
	}

	boolean toBeConsidered () {
		return toBeConsidered;
	}
	
}

enum Genre {
	Film_Noir,
	Action, War, 
	History, Western,
	Documentary, Sport,
	Thriller, News,
	Biography, Adult,
	Comedy, Mystery, 
	Musical, Short, 
	Talk_Show, Adventure,
	Horror, Romance, Sci_Fi,
	Drama, Music, Game_Show,
	Crime, Fantasy, Animation,
	Family, Reality_TV;

	public static Integer indexof(String s) {
		Genre p = Genre.valueOf(s);
		return p.ordinal();
	}
}

/*
enum PrimaryProfession {
	actor, director, writer;
}
*/

enum Professions {
	special_effects, casting_director,
	production_department, miscellaneous,
	animation_department, assistant_director,
	cinematographer, music_department,
	executive, set_decorator,
	art_director, costume_designer, 
	legal, camera_department,
	electrical_department, soundtrack,
	actress, editor, 
	art_department, manager, 
	script_department, director, 
	composer, sound_department, 
	transportation_department, assistant,
	talent_agent, casting_department, 
	editorial_department, stunts, 
	actor, make_up_department, 
	production_designer, location_management,
	producer, writer, 
	visual_effects, production_manager,
	costume_department, publicist;

	public static Integer indexof(String s) {
		Professions p = Professions.valueOf(s);
		return p.ordinal();
	}
}

enum ProfessionCategory {
	actor, editor, actress,
	director, composer,
	production_designer, 
	archive_footage, self, 
	producer, writer, 
	cinematographer, 
	archive_sound;
}

enum TitleTypes {
	videoGame 	(false),
	movie 		(true),
	tvSeries 	(false),
	tvMiniSeries(false),
	short_type 	(false),
	tvSpecial 	(false),
	tvShort 	(false),
	video 		(false),
	tvMovie 	(false),
	tvEpisode 	(false);
	
	public static Integer indexof(String s) {
		TitleTypes p = TitleTypes.valueOf(s);
		return p.ordinal();
	}

	private boolean toBeConsidered;
	
	private TitleTypes(boolean b) {
		toBeConsidered = b;
	}
	
	boolean toBeConsidered () {
		return toBeConsidered;
	}
}

class NameEntry {
	int numMoviesOscar;
	int numMoviesOscarNominations;
	final int numOscar;
	final int numOscarNominations;
	
	int numMoviesFilmFare;
	int numMoviesFilmFareNominations;
	final int numFilmFare;
	final int numFilmFareNominations;
	
	int primaryProfession;
	int isHollywood;
	int medianRating;
	int medianRating_movies;

	public NameEntry(String id2, String primaryName2, int birthYear2, int deathYear2,
			HashSet<Integer> primaryProfession2, HashSet<Integer> knownForTitles2,
			int numOscar2, 		int numOscarNominations2,
			int numFilmFare2, 	int numFilmFareNominations2) {
		id 						= id2;
		birthYear 				= birthYear2;
		deathYear 				= deathYear2;
		primaryName 			= primaryName2;
		knownForTitles    		= knownForTitles2;
		primaryProfessions 		= primaryProfession2;

		numOscar				= numOscar2;
		numOscarNominations		= numOscarNominations2;

		numFilmFare				= numFilmFare2;
		numFilmFareNominations	= numFilmFareNominations2;
	}
	
	public void updatePrimaryProfession(int p) {
		primaryProfession = p;
	}

	public void updateRegion(int numHollywood, int numNonHollywood) {
		if (numHollywood >= numNonHollywood)
			isHollywood = 1;
		else
			isHollywood = 0;
	}

	/** the node corresponding to this name */
	int nodeID;
	public void addGraphNodeID(int nodeID) {
		this.nodeID = nodeID;
	}

	public void updateMedianRating(int rating, int movieRating) {
		this.medianRating 		 = rating;
		this.medianRating_movies = movieRating;
	}

	HashSet<Integer> genre;
	HashSet<Integer> genreMovie;
	
	public void updateGenre(HashSet<Integer> genre, HashSet<Integer> genreMovie) {
		this.genre 		 = genre;
		this.genreMovie  = genreMovie;
	}
	
	//titles associated with the person
	HashSet<Integer> titles = new HashSet<>();
	HashSet<Integer> movies = new HashSet<>();
	
	/**
	 * add this title to the titles associated with the person
	 * @param titleID
	 * @param professionCategory 
	 */
	public void addTitle(int titleID, boolean isMovie,
			ProfessionCategory professionCategory, String title) {
		titles.add(titleID);
		
		boolean oscarNominated = IMDB.oscarNominationsMovies.contains(title); 
		boolean oscarWinner    = IMDB.oscarWinnersMovies.    contains(title);

		boolean filmFareNominated = IMDB.filmFareNominationsMovies.contains(title); 
		boolean filmFareWinner    = IMDB.filmFareWinnersMovies.    contains(title);

		if(isMovie) {
			movies.add(titleID);
			
			if (oscarNominated) {
				numMoviesOscarNominations++;
				if (IMDB.maxNominationMovies < numMoviesOscarNominations)
					IMDB.maxNominationMovies = numMoviesOscarNominations;
				if (oscarWinner)
					numMoviesOscar++;
			}
			
			if (filmFareNominated) {
				numMoviesFilmFareNominations++;
				if (IMDB.maxNominationMovies < numMoviesFilmFareNominations)
					IMDB.maxNominationMovies = numMoviesFilmFareNominations;
				if (filmFareWinner)
					numMoviesFilmFare++;
			}
		}
		
		professionCategoryList.add(professionCategory);
	}

	ArrayList<ProfessionCategory> professionCategoryList = new ArrayList<>();
	String id;
	String primaryName;
	int birthYear,deathYear; //-1 if no value available
	HashSet<Integer> primaryProfessions;
	HashSet<Integer> knownForTitles;
	HashSet<Integer> knownForMovies;
}

class TitleEntry {
	private int rating;
	HashSet<String> names 			= new HashSet<>();
	HashSet<String> namesActor 		= new HashSet<>();
	HashSet<String> namesWriter 	= new HashSet<>();
	HashSet<String> namesDirector 	= new HashSet<>();
	Region region;

	public TitleEntry(int id2, int titleType2, String primaryTitle2, String originalTitle2, boolean isAdult2,
			int startYear2, int endYear2, int runtimeMinutes2, HashSet<Integer> genres2, Region region2) {
		id 				= id2;
		region 			= region2;
		genres 			= genres2;
		endYear 		= endYear2;
		isAdult 		= isAdult2;
		startYear 		= startYear2;
		titleType 		= titleType2;
		primaryTitle 	= primaryTitle2;
		originalTitle 	= originalTitle2;
		runtimeMinutes 	= runtimeMinutes2;
	}
	
	public boolean isMovie() {
		return (titleType == TitleTypes.movie.ordinal());
	}

	public void addPerson(String nameID) {
		names.add(nameID);
	}

	public int rating() {
		return rating;
	}

	public void updateRating(int rating) {
		this.rating = rating;
	}
	
	int id;
	int titleType;
	String primaryTitle;
	String originalTitle;
	boolean isAdult;
	int startYear;
	int endYear;
	int runtimeMinutes;
	HashSet<Integer> genres;
}