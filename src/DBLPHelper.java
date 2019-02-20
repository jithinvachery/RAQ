import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class DBLPHelper {
	public static boolean test = RAQ.test;
	public static int testID = 10000;

	public static boolean Serialize = false;

	public static int edgeThreshold = 1;

	public static boolean DisplayInside = false;
	public static final String fName = "../data/dblp/dblp.txt";
}

/**
 * class used to store data set into memory
 * 
 * @author jithin
 *
 */
class DBLPAlternateEntry {
	final Integer author;
	final HashSet<Integer> neighbours;
	final HashSet<Integer> venues;
	final HashSet<Integer> subjects;
	final ArrayList<Integer> ranks;
	final HashMap<Integer, Integer> venueToNumPapers;
	final ArrayList<Integer> qualitativeRanks;

	private int startYear;
	private int endYear;
	private int numPapers;

	// stat
	private static int maxNumPapers;
	private static int maxActiveYears;

	private DBLPAlternateEntry(Integer author) {
		this.author = author;
		venues = new HashSet<>();
		subjects = new HashSet<>();
		ranks = new ArrayList<>();
		startYear = Integer.MAX_VALUE;
		endYear = Integer.MIN_VALUE;
		numPapers = 0;
		neighbours = new HashSet<>();
		venueToNumPapers = new HashMap<>();
		qualitativeRanks = new ArrayList<>();
	}

	/**
	 * Loads data-set into memory and also calculates some stat
	 * 
	 * @param fName
	 * @return
	 * @throws IOException
	 */
	static TreeMap<Integer, DBLPAlternateEntry> loadDataset(String fName) throws IOException {
		ArrayList<DBLPEntry> rawData = DBLPEntry.loadDataset(fName);
		TreeMap<Integer, DBLPAlternateEntry> map = new TreeMap<>();

		for (DBLPEntry raw : rawData) {
			HashSet<Integer> authors = raw.Authors();

			for (Integer author : authors) {
				DBLPAlternateEntry entry = map.get(author);

				if (entry == null) {
					entry = new DBLPAlternateEntry(author);
					map.put(author, entry);
				}

				entry.Update(raw.Year(), raw.Rank(), raw.Venue(), raw.Subjects());

				// keep a note of neighbours
				for (Integer author2 : authors) {
					if (author > author2)
						entry.neighbours.add(author2);
				}
			}
		}
		rawData = null;

		return map;
	}

	/**
	 * update the details of all authors
	 * 
	 * @param year
	 * @param rank
	 * @param venue
	 * @param subjects
	 */
	private void Update(int year, int rank, Integer venue, HashSet<Integer> subjects) {
		// being called means we have a new paper
		numPapers++;

		if (numPapers > maxNumPapers)
			maxNumPapers = numPapers;

		if (year > endYear)
			endYear = year;
		if (year < startYear)
			startYear = year;

		int a = ActiveYears();
		if (a > maxActiveYears)
			maxActiveYears = a;

		ranks.add(rank);
		venues.add(venue);
		this.subjects.addAll(subjects);

		// update venue count
		// is the venue a conference
		Integer num = venueToNumPapers.get(venue);

		if (num == null)
			num = 0;

		num++;
		venueToNumPapers.put(venue, num);

		if (DBLPEntry.IsConference(venue)) {
			if (rank < 5)
				qualitativeRanks.add(rank);
		}
	}

	int ActiveYears() {
		int ret = endYear - startYear + 1;
		if (ret <= 0)
			ret = 0;

		return ret;
	}

	public HashSet<Integer> Venues() {
		return venues;
	}

	public HashSet<Integer> Subjects() {
		return subjects;
	}

	public int MedianRank() {
		return Helper.Median(ranks);
	}

	public int NumPapers() {
		return numPapers;
	}

	public int AuthorID() {
		return author;
	}

	public static int NumSubjects() {
		return DBLPEntry.NumSubjects();
	}

	public static int MaxYearsActive() {
		return maxActiveYears;
	}

	public static int NumRanks() {
		return DBLPEntry.NumRanks();
	}

	public static int MaxPapersPerAuthor() {
		return maxNumPapers;
	}

	public static int NumVenues() {
		return DBLPEntry.NumVenues();
	}

	public static int NumDiffPapers() {
		return DBLPEntry.NumPapers();
	}

	public static void Destroy() {
		DBLPEntry.Destroy();
	}

	
	public Integer MostPublishedVenue() {
		Integer venueID = null;
		Integer max = -1;

		for (Map.Entry<Integer, Integer> entry : venueToNumPapers.entrySet()) {
			Integer num = entry.getValue();
			if (num > max) {
				Integer id = entry.getKey();
				max = num;
				venueID = id;
			}
		}

		return venueID;
	}

	/**
	 * return the most published non-empty venue
	 * 
	 * @return
	 */
	public Integer MostPublishedVenueNonEmpty() {
		Integer venueID = null;
		Integer max = -1;

		for (Map.Entry<Integer, Integer> entry : venueToNumPapers.entrySet()) {
			Integer num = entry.getValue();
			if (num > max) {
				Integer id = entry.getKey();
				if (DBLPEntry.GetVenueName(id).length() == 0) {
					continue;
				} else {
					max = num;
					venueID = id;
				}
			}
		}

		return venueID;
	}

	/**
	 * return the most published non-empty venue, if only empty venue available
	 * for user, return that
	 * 
	 * @return
	 */
	public Integer MostPublishedRankedVenue() {
		Integer venueID = null;
		Integer max = -1;

		for (Map.Entry<Integer, Integer> entry : venueToNumPapers.entrySet()) {
			Integer num = entry.getValue();
			if (num > max) {
				Integer id = entry.getKey();
				String venueName = DBLPEntry.GetVenueName(id);

				if (ConferenceInfo.IsRanked(venueName)) {
					max = num;
					venueID = id;
				}
			}
		}
		return venueID;
	}

	public Integer MostPublishedIfPossibleRankedVenue() {
		Integer ret = MostPublishedRankedVenue();

		if (ret == null) {
			ret = MostPublishedVenueNonEmpty();
		}

		if (ret == null) {
			ret = MostPublishedVenue();
		}

		return ret;
	}

	public HashSet<Integer> SubjectsOfVenue(Integer venuID) {
		String venueName = DBLPEntry.GetVenueName(venuID);
		return ConferenceInfo.GetSubjectsOfVenue(venueName);
	}

	public int MedianRankCoreConference() {
		int ret;

		if (qualitativeRanks.size() == 0)
			ret = 5;
		else
			ret = Helper.Median(qualitativeRanks);

		return ret;
	}
}

/**
 * class used to store data set into memory
 * 
 * @author jithin
 *
 */
class DBLPEntry implements Comparable<DBLPEntry>, Comparator<DBLPEntry> {
	private static HashSet<String> Authors;
	private static HashSet<String> Venue;
	private static HashSet<String> PaperId;
	private static HashSet<Integer> Years;
	private static int numYears;
	private static int maxNumAuthors;
	private static boolean staticsInitiated = false;

	private static boolean mapCreated;
	private static HashMap<String, Integer> AuthorMap;
	private static HashMap<String, Integer> VenueMap;
	private static HashMap<String, Integer> PaperIdMap;
	private static HashMap<Integer, Integer> YearMap;
	private static HashMap<String, Integer> PaperIdToYearMap;

	// maps used to reverse mapping
	private static HashMap<Integer, String> AuthorMapReverse;
	private static HashMap<Integer, String> VenueMapReverse;
	private static HashMap<Integer, String> PaperIdMapReverse;
	private static HashMap<Integer, Integer> YearMapReverse;

	// work on partial dataset
	static final boolean test = DBLPHelper.test;
	static final int testID = DBLPHelper.testID;

	private final int year;
	private final String venue;
	private final String paperId;
	private final HashSet<String> authors;
	private final HashSet<String> reference;

	public static int NumSubjects() {
		return ConferenceInfo.NumSubjects();
	}

	public static boolean IsConference(Integer venueID) {
		String venueName = VenueMapReverse.get(venueID);
		return ConferenceInfo.IsConference(venueName);
	}

	public static int NumRanks() {
		return ConferenceInfo.NumRanks();
	}

	private static void InitStatics() {
		Authors = new HashSet<>();
		Venue 	= new HashSet<>();
		PaperId = new HashSet<>();
		Years 	= new HashSet<>();
		
		numYears		= -1;
		maxNumAuthors 	= -1;
		
		mapCreated 	= false;
		AuthorMap	= new HashMap<>();
		VenueMap 	= new HashMap<>();
		PaperIdMap 	= new HashMap<>();
		YearMap 	= new HashMap<>();
		PaperIdToYearMap = new HashMap<>();

		staticsInitiated = true;
	}

	private DBLPEntry(HashSet<String> authors, int year, String venue, String id, HashSet<String> reference) {

		this.paperId= id;
		this.year 	= year;
		this.venue 	= venue;
		this.authors= authors;
		this.reference = reference;

		int numAuthors = authors.size();
		
		if (maxNumAuthors < numAuthors)
			maxNumAuthors = numAuthors;
		
		Authors.addAll	(authors);
		Venue.add		(venue);
		PaperId.add		(id);

		Years.add(year);

		PaperIdToYearMap.put(id, year);
	}

	/**
	 * Constructor
	 * 
	 * @param authors
	 * @param year
	 * @param venue
	 * @param id
	 * @param reference
	 * @return null if this is a duplicate entry
	 */
	static DBLPEntry GetDBLPEntry(HashSet<String> authors, int year, String venue, String id,
			HashSet<String> reference) {
		DBLPEntry ret = null;

		if (!staticsInitiated) {
			InitStatics();
		}
		if (!PaperId.contains(id)) {
			ret = new DBLPEntry(authors, year, venue, id, reference);
		} else {
			// duplicate entry so discard
		}

		return ret;
	}

	public Integer YearProb() {
		return YearMap.get(year);
	}

	public Integer PaperID() {
		return PaperIdMap.get(paperId);
	}

	public int Year() {
		return year;
	}

	public HashSet<Integer> Authors() {
		HashSet<Integer> ret = new HashSet<>();

		for (String author : authors)
			ret.add(AuthorMap.get(author));

		return ret;
	}

	public Integer Venue() {
		return VenueMap.get(venue);
	}

	public int Rank() {
		return ConferenceInfo.GetRankOfVenue(venue);
	}

	public HashSet<Integer> Subjects() {
		return ConferenceInfo.GetSubjectsOfVenue(venue);
	}

	/**
	 * We need to collate the stats into hashMaps function to be called only
	 * once
	 */
	public static void UpdateMap() {
		UpdateMap(null);
	}
	
	public static void UpdateMap(ArrayList<Integer> nodePermutation) {
		if (mapCreated) {
			System.err.println("We have created the map already");
			System.exit(-1);
		}

		int uid;

		// we want AuthourID and PaperID to be consistent
		// since we are using it for serializing
		ArrayList<String> strArr;

		strArr = new ArrayList<>(Authors);
		Collections.sort(strArr);
		uid = 0;
		for (String author : strArr)
			AuthorMap.put(author, uid++);

		strArr = new ArrayList<>(Venue);
		Collections.sort(strArr);
		uid = 0;
		for (String venue : strArr)
			VenueMap.put(venue, uid++);

		strArr = new ArrayList<>(PaperId);
		Collections.sort(strArr);
		uid = 0;
		
		if (nodePermutation != null) {
			for (Integer index : nodePermutation) {
				PaperIdMap.put(strArr.get(index), uid++);
			}
		} else {
			for (String id : strArr)
				PaperIdMap.put(id, uid++);
		}
		
		ArrayList<Integer> yearsArr = new ArrayList<>(Years);
		Collections.sort(yearsArr);
		uid = 0;
		for (Integer year : yearsArr)
			YearMap.put(year, uid++);

		// update numYears
		numYears = Years.size();

		// freeing memory
		Authors = null;
		Years = null;
		Venue = null;
		PaperId = null;

		mapCreated = true;

		// now we shall create the reverse map
		AuthorMapReverse = new HashMap<>();
		VenueMapReverse = new HashMap<>();
		PaperIdMapReverse = new HashMap<>();
		YearMapReverse = new HashMap<>();

		for (Map.Entry<String, Integer> entry : AuthorMap.entrySet()) {
			AuthorMapReverse.put(entry.getValue(), entry.getKey());
		}

		for (Map.Entry<String, Integer> entry : VenueMap.entrySet()) {
			VenueMapReverse.put(entry.getValue(), entry.getKey());
		}

		for (Map.Entry<String, Integer> entry : PaperIdMap.entrySet()) {
			PaperIdMapReverse.put(entry.getValue(), entry.getKey());
		}

		for (Map.Entry<Integer, Integer> entry : YearMap.entrySet()) {
			YearMapReverse.put(entry.getValue(), entry.getKey());
		}
	}

	public static String GetAuthorName(Integer authoutID) {
		return AuthorMapReverse.get(authoutID);
	}

	public static String GetVenueName(Integer venueID) {
		return VenueMapReverse.get(venueID);
	}

	public static String GetPaperIDName(Integer paperID) {
		return PaperIdMapReverse.get(paperID);
	}

	public static Integer GetYear(Integer para) {
		return YearMapReverse.get(para);
	}

	public static int NumAuthors() {
		Check();
		return AuthorMap.size();
	}

	private static void Check() {
		if (!mapCreated) {
			System.err.println("We have not created the map yet");
			System.exit(-1);
		}
	}

	public static int NumVenues() {
		Check();
		return VenueMap.size();
	}

	public static int NumYears() {
		Check();
		return numYears;
	}

	/**
	 * returns the nodeid of neighbours
	 * 
	 * @return
	 */
	public HashSet<Integer> Neighbours(double frac) {
		HashSet<Integer> ref = new HashSet<>();

		for (String r : reference) {
			Integer id = PaperIdMap.get(r);
			if (DBLP.test || frac != 1.0) {
				if (id == null)
					continue;
			}

			ref.add(id);
		}

		return ref;
	}

	public int NodeID() {
		return PaperIdMap.get(paperId);
	}

	/**
	 * year of publication of all neighbours
	 * 
	 * @return
	 */
	public HashSet<Integer> NeighboursYear(double frac) {
		HashSet<Integer> ref = new HashSet<>();

		for (String r : reference) {
			Integer id = PaperIdToYearMap.get(r);
			if (DBLP.test || frac != 1.0) {
				if (id == null)
					continue;
			}

			ref.add(id);
		}

		return ref;
	}

	/**
	 * Loads data-set into memory and also calculates some stat
	 * 
	 * @param fName
	 * @return
	 * @throws IOException
	 */
	static ArrayList<DBLPEntry> loadDataset(String fName) throws IOException {
		return loadDataset (fName, null);
	}
	
	static ArrayList<DBLPEntry> loadDataset(String fName,
				ArrayList<Integer> nodePermutation) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fName));
		ArrayList<DBLPEntry> dblpEntries = new ArrayList<>(2700000);
		String l;
		ArrayList<Integer> degrees = new ArrayList<>();

		int b = 0;
		while ((l = br.readLine()) != null) {
			HashSet<String> authors = new HashSet<>();
			int year = 0;
			String venue = "";
			String id = "";
			HashSet<String> reference = new HashSet<>();

			if (test) {
				if (b++ == testID)
					break;
			}
			do {
				switch (l.charAt(1)) {
				case '@':
					// authors
					String authorsString = l.substring(2);
					String authorsSplit[] = authorsString.split(",");
					// all strings except first have a extra 'space' at
					// beginning
					for (int i = 0; i < authorsSplit.length; i++) {
						authors.add(authorsSplit[i].trim());
					}

					break;
				case 't':
					// Year
					year = Integer.parseInt(l.substring(2));
					break;
				case 'c':
					// VENUE
					venue = ConferenceInfo.RemoveQuotes(l.substring(2).trim());
					venue = venue.replace(",", "");
					if(venue.length() == 0)
						System.out.println("how");
					break;
				case 'i':
					// ID
					id = l.substring(6).trim();
					break;
				case '%':
					// referred paper
					reference.add(l.substring(2).trim());
					break;
				default:
					break;
				}

				l = br.readLine();
				ShowProgress.Show();
			} while ((l != null) && (l.length() != 0));

			// create an entry
			DBLPEntry entry = GetDBLPEntry(authors, year, venue, id, reference);
			if (entry != null) {
				dblpEntries.add(entry);
				degrees.add(reference.size());
			}
		}

		// generate hashmaps to get author_id etc
		DBLPEntry.UpdateMap(nodePermutation);
		br.close();

		//Helper.SortAndPrintStat(degrees);
		return dblpEntries;
	}

	public static int NumPapers() {
		return PaperIdMap.size();
	}

	public static void Destroy() {
		Authors = null;
		Venue 	= null;
		Years 	= null;
		// AuthorMap = null;
		VenueMap 	= null;
		PaperIdMap 	= null;
		YearMap 	= null;
		PaperIdToYearMap = null;

		staticsInitiated = false;
	}

	@Override
	public int compare(DBLPEntry o1, DBLPEntry o2) {
		return o1.paperId.compareTo(o2.paperId);
	}

	@Override
	public int compareTo(DBLPEntry o) {
		return compare(this, o);
	}

	public static Integer GetAuthorID(String author) {
		return AuthorMap.get(author);
	}


	public static int MaxNumAuthors() {
		return maxNumAuthors;
	}
}

class ConferenceInfo {
	private static final int numRanks = 5;
	private static int numSubjects = -1;
	private static ArrayList<HashSet<String>> 			ranking 			= new ArrayList<>(5);
	private static HashMap<String, HashSet<Integer>> 	venueToSubject 		= new HashMap<>();
	private static HashMap<Integer, Integer> 			subjectToSubject 	= new HashMap<>();
	private static HashSet<Integer> 					subjects 			= new HashSet<>();
	private static HashSet<String> 						conferences 		= new HashSet<>();

	private static HashMap<Integer, Integer>subjectToSubjectReverse = new HashMap<>();
	private static HashSet<String> 			VenuesNotRanked 		= new HashSet<>();

	static void Destroy() {
		ranking 		= new ArrayList<>(5);
		venueToSubject 	= new HashMap<>();
		subjectToSubject= new HashMap<>();
		subjects 		= new HashSet<>();
		VenuesNotRanked = new HashSet<>();
	}

	public static boolean IsRanked(String venueName) {
		boolean ret = true;
		
		if (VenuesNotRanked.contains(venueName)) {
			ret = false;
		}
		
		return ret;
	}

	public static boolean IsConference(String venueName) {
		return conferences.contains(venueName);
	}

	public static int NumSubjects() {
		return numSubjects;
	}

	public static Integer GetSubjectReverse(int subject) {
		return subjectToSubjectReverse.get(subject);
	}

	static void Init(String fName) throws IOException {
		for (int i = 0; i < numRanks; i++)
			ranking.add(new HashSet<String>());

		LoadConferenceRanking("../data/dblp/CORE.csv");
		LoadJournalRanking("../data/dblp/CORE_journals.csv");

		// convert the subjects into a list of contiguous integer
		UpdateSubjects();
	}

	public static HashSet<Integer> GetSubjectsOfVenue(String venue) {
		HashSet<Integer> ret = venueToSubject.get(venue);

		// this happens if the venue is not in the CORE dataset
		if (ret == null)
			ret = new HashSet<>();

		return ret;
	}

	public static int GetRankOfVenue(String venue) {
		int i = 0;

		for (HashSet<String> s : ranking) {
			i++;
			if (s.contains(venue))
				break;

			else if (i == numRanks) {
				VenuesNotRanked.add(venue);
			}
		}
		// it would return he max value if we have not saved the venue
		return i;
	}

	// UNNECCESSARY
	static void ShowUnRankedVenues() {
		System.out.println("\nUnranked conferences are : ");
		ArrayList<String> arr = new ArrayList<>(VenuesNotRanked);
		Collections.sort(arr);
		for (String s : arr)
			System.out.println(s);
		System.out.println(" ++++++++++++++++++++++++++++++++++++++++++++++ ");
		/*
		 * System.out.println("Ranked Venues"); int i=1; for (HashSet<String> s
		 * : ranking) { System.out.println("Rank : "+i++);
		 * 
		 * for (String r : s) { System.out.print(r+"{}"); }
		 * System.out.println(); }
		 */
	}

	private static void UpdateSubjects() {
		ArrayList<Integer> subjectsList = new ArrayList<>(subjects);
		// free memory
		numSubjects = subjects.size();
		subjects = null;

		Collections.sort(subjectsList);

		Integer i = 0;
		for (Integer s : subjectsList) {
			subjectToSubjectReverse.put(i, s);
			subjectToSubject.put(s, i++);
		}

		// update the venueToSubject mapping
		HashMap<String, HashSet<Integer>> venueToSubjectTemp = new HashMap<>();

		for (Entry<String, HashSet<Integer>> entry : venueToSubject.entrySet()) {
			HashSet<Integer> newList = new HashSet<>();

			for (Integer s : entry.getValue()) {
				newList.add(subjectToSubject.get(s));
			}

			venueToSubjectTemp.put(entry.getKey(), newList);
		}

		venueToSubject = venueToSubjectTemp;

		// freeing memory
		subjectToSubject = null;
	}

	private static ArrayList<HashSet<String>> LoadJournalRanking(String fName) throws IOException {
		return LoadRanking(fName, false, 1, -1, 3, 5);
	}

	private static ArrayList<HashSet<String>> LoadConferenceRanking(String fName) throws IOException {
		return LoadRanking(fName, true, 1, 2, 4, 6);
	}

	private static ArrayList<HashSet<String>> LoadRanking(String fName, boolean conference, int icName, int icAcronym,
			int icRank, int icSubject) throws NumberFormatException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(fName));

		String l;
		// skip the first line
		br.readLine();

		while ((l = br.readLine()) != null) {
			// split on the comma only if that comma has zero, or an even number
			// of quotes ahead of it.
			String s[] = l.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

			int limit;
			if (conference)
				limit = 9;
			else
				limit = 12;

			if (s.length < limit) {
				// we do not have enough values
				System.err.println("Fatal error s.length  = " + s.length + " LoadConferenceRanking : " + fName);
				System.exit(-1);
			}

			String cName;
			String cAcronym;
			String cRank;

			// conference name
			cName = RemoveQuotes(s[icName]);
			cRank = s[icRank].trim();

			// subject
			HashSet<Integer> cSubjectI = new HashSet<>();
			int size;
			if (conference)
				size = 1;
			else
				size = 3;

			for (int i = 0; i < size; i++) {
				String cSubject = s[icSubject + i].trim();

				if (cSubject.length() == 0)
					continue;

				cSubjectI.add(Integer.parseInt(cSubject));
			}
			subjects.addAll(cSubjectI);

			int cRankI = GetRank(cRank);
			HashSet<String> set = ranking.get(cRankI - 1);

			// populate venue to subject map
			venueToSubject.put(cName, cSubjectI);
			if (conference) {
				cAcronym = RemoveQuotes(s[icAcronym]);
				if (cAcronym.length() > 0) {
					venueToSubject.put(cAcronym, cSubjectI);
					set.add(cAcronym);
				}
			}
			set.add(cName);

			if (conference)
				conferences.add(cName);
		}
		br.close();

		return null;
	}

	/**
	 * Function remove Quotes in the name and also "(numeral)"
	 * 
	 * @param s
	 * @return
	 */
	static String RemoveQuotes(String s) {
		s = s.trim();
		if (s.length() > 0 && s.charAt(0) == '\"') {
			s = s.substring(1, s.length() - 1).trim();
		}
		if (s.length() > 0 && s.charAt(s.length() - 1) == ')') {
			int i;
			for (i = s.length() - 2; i > 0; i--) {
				if (s.charAt(i) == '(')
					break;
			}

			if (i >= 0) {
				// check if the thing in between parenthesis is numeral
				String num = s.substring(i + 1, s.length() - 1);
				try {
					Integer.parseInt(num);
					s = s.substring(0, i).trim();
				} catch (NumberFormatException e) {
					// since not a number we do nothing
				}
			}
		}
		return s;
	}

	private static int GetRank(String cRank) {
		int ret;
		switch (cRank) {
		case "A*":
			ret = 1;
			break;
		case "A":
			ret = 2;
			break;
		case "B":
			ret = 3;
			break;
		case "C":
			ret = 4;
			break;
		default:
			ret = 5;
			break;
		}
		if (ret > numRanks) {
			System.err.println("Fatal error DBLP.ConferenceInfo.getRank");
		}
		return ret;
	}

	public static int NumRanks() {
		return numRanks;
	}

}
