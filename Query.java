import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.io.FileInputStream;
import java.util.*;

/**
 * Runs queries against a back-end database
 */
public class Query {
	private String configFilename;
	private Properties configProps = new Properties();

	private String postgreSQLDriver;
	private String postgreSQLIMDBUrl;
	private String postgreSQLCustomerUrl;
	private String postgreSQLUser;
	private String postgreSQLPassword;

	// DB Connection
	private Connection imdb_conn;
	private Connection customer_conn;

	// Canned queries
	private static final String SQL_SEARCH =
		"SELECT * FROM movie WHERE name ILIKE ? ORDER BY id;";
	private PreparedStatement movieSearchStatement;

	private static final String SQL_FAST_SEARCH = 
		" SELECT mov.id, mov.name, mov.year, mov.fname as afname, mov.lname as alname, dir.fname, dir.lname" +
		" FROM (SELECT m.id, d.fname, d.lname" +
		" FROM (SELECT * FROM movie x WHERE x.name ILIKE ?) AS m" +
		" LEFT JOIN Movie_Directors md ON m.id = md.mid" +
		" LEFT JOIN Directors d ON d.id = md.did) AS dir," +
		" (select m.id, m.name, m.year, a.fname, a.lname" +
		" FROM (SELECT * FROM movie x WHERE x.name ILIKE ?) AS m" +
		" LEFT JOIN Casts c ON m.id = c.mid" +
		" LEFT JOIN Actor a ON c.pid = a.id) AS mov" +
		" WHERE dir.id = mov.id ORDER BY dir.id";
	private PreparedStatement fastSearchStatement;
	
	private static final String SQL_RENT_MOVIE =
		"INSERT INTO Rental VALUES(?, TRUE, ?, ?);";
	private PreparedStatement customerRentMovieStatement;
	
	private static final String SQL_RETURN_MOVIE =
		"UPDATE rental SET status = false WHERE customerid = ? AND movieid = ?;";
	private PreparedStatement customerReturnMovieStatement;
	
	private static final String DIRECTOR_MID_SQL = "SELECT y.* "
					 + "FROM movie_directors x, directors y "
					 + "WHERE x.mid = ? and x.did = y.id";
	private PreparedStatement directorMidStatement;
	
	private static final String ACTOR_MID_SQL = "SELECT a.fname, a.lname "
		 + "FROM Actor a, Casts c, Movie m "
		 + "WHERE a.id = c.pid and m.id = c.mid and m.id = ?;";
	private PreparedStatement actorMidStatement;
	
	private static final String MOVIE_RENTAL_SQL = 
		"SELECT * FROM Rental WHERE movieid = ? and status = 'true';";
	private PreparedStatement movieRentalStatement;
	
	private static final String MOVIE_RETURN_SQL = 
		"SELECT * FROM Rental WHERE movieid = ? and status = 'true' and customerid = ?;";
	private PreparedStatement movieReturnStatement;

	private static final String VALID_MOVIE_SQL = 
		"SELECT * FROM Movie WHERE id = ?;";
	private PreparedStatement validMovieStatement;
	
	private static final String VALID_PLAN_SQL = 
		"SELECT * FROM Plan WHERE pid = ?;";
	private PreparedStatement validPlanStatement;
	
	private static final String UPDATE_PLAN_SQL = 
		"UPDATE Customer SET planid = ? WHERE cid = ?;";
	private PreparedStatement updatePlanStatement;
	
	private static final String CUSTOMER_LISTPLANS_SQL = 
		"SELECT * FROM Plan;";
	private PreparedStatement customerListPlansStatement;

	private static final String CUSTOMER_LOGIN_SQL = 
		"SELECT * FROM customer WHERE login = ? and password = ?;";
	private PreparedStatement customerLoginStatement;
	
	private static final String CUSTOMER_GETNAME_SQL = 
		"SELECT fname,lname FROM customer WHERE cid = ?;";
	private PreparedStatement customerGetNameStatement;
	
	private static final String CUSTOMER_GETPLAN_SQL = 
		"SELECT p.maxrental FROM customer c, plan p WHERE cid = ? AND c.planid = p.pid;";
	private PreparedStatement customerGetPlanStatement;
	
	private static final String CUSTOMER_GETRENTALSCOUNT_SQL = 
		"SELECT COUNT(*) FROM Rental WHERE customerid = ? AND status = 'true';";
	private PreparedStatement customerGetRentalsCountStatement;
	
	private static final String BEGIN_TRANSACTION_SQL = 
		"BEGIN TRANSACTION READ WRITE ISOLATION LEVEL SERIALIZABLE";
	private PreparedStatement beginCustomerTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT";
	private PreparedStatement commitCustomerTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK";
	private PreparedStatement rollbackCustomerTransactionStatement;
	
	private static final String LOCK_CUSTOMER_TABLE_SQL = "LOCK TABLE CUSTOMER;";
	private PreparedStatement lockCustomerTableStatement;

	public Query(String configFilename) {
		this.configFilename = configFilename;
	}

    /**********************************************************/
    /* Connections to postgres */

	public void openConnection() throws Exception {
		configProps.load(new FileInputStream(configFilename));

		postgreSQLDriver   = configProps.getProperty("videostore.jdbc_driver");
		postgreSQLIMDBUrl	   = configProps.getProperty("videostore.imdb_url");
		postgreSQLCustomerUrl = configProps.getProperty("videostore.customer_url");
		postgreSQLUser	   = configProps.getProperty("videostore.postgres_username");
		postgreSQLPassword = configProps.getProperty("videostore.postgres_password");


		/* load jdbc drivers */
		Class.forName(postgreSQLDriver).newInstance();

		/* open connections to the imdb database */
		imdb_conn = DriverManager.getConnection(postgreSQLIMDBUrl, // database
				postgreSQLUser, // user
				postgreSQLPassword); // password
		
		
		/* open connections to the customer database */
		customer_conn = DriverManager.getConnection(postgreSQLCustomerUrl, // database
						postgreSQLUser, // user
						postgreSQLPassword); // password
	}

	public void closeConnection() throws Exception {
		imdb_conn.close();
		customer_conn.close();
	}

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {

		// imdb queries
		directorMidStatement = imdb_conn.prepareStatement(DIRECTOR_MID_SQL);
		actorMidStatement = imdb_conn.prepareStatement(ACTOR_MID_SQL);
		movieSearchStatement = imdb_conn.prepareStatement(SQL_SEARCH);
		validMovieStatement = imdb_conn.prepareStatement(VALID_MOVIE_SQL);
		fastSearchStatement = imdb_conn.prepareStatement(SQL_FAST_SEARCH);
		
		// customer queries
		movieReturnStatement = customer_conn.prepareStatement(MOVIE_RETURN_SQL);
		customerReturnMovieStatement = customer_conn.prepareStatement(SQL_RETURN_MOVIE);
		customerRentMovieStatement = customer_conn.prepareStatement(SQL_RENT_MOVIE);
		movieRentalStatement = customer_conn.prepareStatement(MOVIE_RENTAL_SQL);
		validPlanStatement = customer_conn.prepareStatement(VALID_PLAN_SQL);
		customerListPlansStatement = customer_conn.prepareStatement(CUSTOMER_LISTPLANS_SQL);
		updatePlanStatement = customer_conn.prepareStatement(UPDATE_PLAN_SQL);
		customerLoginStatement = customer_conn.prepareStatement(CUSTOMER_LOGIN_SQL);
		customerGetNameStatement = customer_conn.prepareStatement(CUSTOMER_GETNAME_SQL);
		customerGetRentalsCountStatement = customer_conn.prepareStatement(CUSTOMER_GETRENTALSCOUNT_SQL);
		customerGetPlanStatement = customer_conn.prepareStatement(CUSTOMER_GETPLAN_SQL);
		customerGetRentalsCountStatement = customer_conn.prepareStatement(CUSTOMER_GETRENTALSCOUNT_SQL);
		
		// transactions
		beginCustomerTransactionStatement = customer_conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitCustomerTransactionStatement = customer_conn.prepareStatement(COMMIT_SQL);
		rollbackCustomerTransactionStatement = customer_conn.prepareStatement(ROLLBACK_SQL);
		
		// table locks
		lockCustomerTableStatement = customer_conn.prepareStatement(LOCK_CUSTOMER_TABLE_SQL);
	}


    /**********************************************************/
    /* Suggested helper functions; you can complete these, or write your own
       (but remember to delete the ones you are not using!) */

	// NOTE: this function should always be called from within a transaction
	public int getRemainingRentals(int cid) throws Exception {
		/* How many movies can she/he still rent?
		   You have to compute and return the difference between the customer's plan
		   and the count of outstanding rentals */
		
		int maxRentals = 0;
		int currentRentals = 0;
		
		customerGetPlanStatement.clearParameters();
		customerGetPlanStatement.setInt(1,cid);
		ResultSet cid_set = customerGetPlanStatement.executeQuery();
		if (cid_set.next()) 
		{
			maxRentals = cid_set.getInt(1);
		}
		
		customerGetRentalsCountStatement.clearParameters();
		customerGetRentalsCountStatement.setInt(1,cid);
		cid_set = customerGetRentalsCountStatement.executeQuery();
		if (cid_set.next()) 
		{
			currentRentals = cid_set.getInt(1);
		}
		
		return (maxRentals - currentRentals);
	}

	public String getCustomerName(int cid) throws Exception {
		/* Find the first and last name of the current customer. */
		
		String fname = "";
		String lname = "";
		
		customerGetNameStatement.clearParameters();
		customerGetNameStatement.setInt(1,cid);

		ResultSet cid_set = customerGetNameStatement.executeQuery();
		if (cid_set.next()) 
		{
			fname = cid_set.getString(1);
			lname = cid_set.getString(2);
		}
			
		return (fname + " " + lname);

	}

	public boolean isValidPlan(int planid) throws Exception {
		/* Is planid a valid plan ID?  You have to figure it out */
		
		validPlanStatement.clearParameters();
		validPlanStatement.setInt(1,planid);
		
		ResultSet plan_set = validPlanStatement.executeQuery();
		if(plan_set.next())
		{
			return true;
		}
		
		return false;
	}

	public boolean isValidMovie(int mid) throws Exception {
		
		validMovieStatement.clearParameters();
		validMovieStatement.setInt(1,mid);
		
		ResultSet movie_set = validMovieStatement.executeQuery();
		if(movie_set.next())
		{
			return true;
		}
		
		return false;
	}
	
	private void PrintUniqueActorsAndDirectors(List<String> listActors, List<String> listDirectors)
	{
		HashSet<String> set = new HashSet<String>();
		
		if(listDirectors != null)
		{
			for (int i = 0; i < listDirectors.size(); i++)
			{
				boolean val = set.add(listDirectors.get(i));
				if (val == true) 
				{
					System.out.println("\tDirector: " + listDirectors.get(i));
				}
			}
		}
		
		set = new HashSet<String>();
		if(listActors != null)
		{
			for (int i = 0; i < listActors.size(); i++)
			{
				boolean val = set.add(listActors.get(i));
				if (val == true) 
				{
					System.out.println("\tActor: " + listActors.get(i));
				}
			}
		}
	}

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
	public int transaction_login(String name, String password) throws Exception {
		/* authenticates the user, and returns the user id, or -1 if authentication fails */

		int cid;

		customerLoginStatement.clearParameters();
		customerLoginStatement.setString(1,name);
		customerLoginStatement.setString(2,password);
		
		ResultSet cid_set = customerLoginStatement.executeQuery();
		
		if (cid_set.next()) 
		{
			cid = cid_set.getInt(1);
		}
		else 
		{
			cid = -1;
		}
		
		return(cid);
	}

	public void transaction_printPersonalData(int cid) throws Exception {
		/* println the customer's personal data: name, and plan number */
		
		beginCustomerTransactionStatement.executeUpdate();
		System.out.println("\n" + getCustomerName(cid) + ", you have " + getRemainingRentals(cid) + " rentals available.\n");
		commitCustomerTransactionStatement.executeUpdate();
	}


    /**********************************************************/
    /* main functions in this project: */

	public void transaction_search(int cid, String movie_title)
			throws Exception {
		/* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
		/* prints the movies, directors, actors, and the availability status:
		   AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

		beginCustomerTransactionStatement.executeUpdate();
		
		// TASK 2D: use prepared binding so JDBC does the escaping
		movieSearchStatement.clearParameters();
		String searchString = "%" + movie_title + "%"; 
		movieSearchStatement.setString(1,searchString);
			
		ResultSet movie_set = movieSearchStatement.executeQuery();
		while (movie_set.next()) 
		{
			int mid = movie_set.getInt(1);
			System.out.println("ID: " + mid + "\n NAME: "
					+ movie_set.getString(2) + "\n YEAR: "
					+ movie_set.getString(3));
			
			/* do a dependent join with directors */
			directorMidStatement.clearParameters();
			directorMidStatement.setInt(1, mid);
			ResultSet director_set = directorMidStatement.executeQuery();
			while (director_set.next()) 
			{
				System.out.println("\tDirector: " + director_set.getString(2)
						+ " " + director_set.getString(3));
			}
			director_set.close();
			
			/* now you need to retrieve the actors, in the same manner */
			actorMidStatement.clearParameters();
			actorMidStatement.setInt(1,mid);
			ResultSet actor_set = actorMidStatement.executeQuery();
			while(actor_set.next())
			{
				System.out.println("\tActor: " + actor_set.getString(1)
						+ " " + actor_set.getString(2));
			}
			actor_set.close();
			
			/* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
			movieRentalStatement.clearParameters();
			movieRentalStatement.setInt(1,mid);
			ResultSet movierental_set = movieRentalStatement.executeQuery();
			
			int rCount = 0;
			while(movierental_set.next())
			{
				rCount++;
				
				if(movierental_set.getInt(1) == cid)
				{
					System.out.println("YOU HAVE IT");
				}
				else
				{
					System.out.println("UNAVAILABLE");
				}
				
				break;
			}
			movierental_set.close();
			
			if(rCount == 0)
			{
				System.out.println("AVAILABLE");
			}

			System.out.println();
		}

		movie_set.close();
		
		commitCustomerTransactionStatement.executeUpdate();
	}

	public void transaction_choosePlan(int cid, int pid) throws Exception {
	    /* updates the customer's plan to pid: UPDATE customer SET plid = pid */
	    /* remember to enforce consistency ! */
		
		beginCustomerTransactionStatement.executeUpdate();
		lockCustomerTableStatement.executeUpdate();
		
		updatePlanStatement.clearParameters();
		updatePlanStatement.setInt(1,pid);
		updatePlanStatement.setInt(2,cid);
		updatePlanStatement.executeUpdate();
		
		int remainingRentals = getRemainingRentals(cid);
		if(remainingRentals < 0)
		{
			rollbackCustomerTransactionStatement.executeUpdate();
			System.out.println("You have too many rentals to change to plan " + pid + ". Request DENIED");
		}
		else
		{
			commitCustomerTransactionStatement.executeUpdate();
			System.out.println("You have changed to plan: " + pid);
		}
	}

	public void transaction_listPlans() throws Exception {
	    /* println all available plans: SELECT * FROM plan */
		
		ResultSet plan_set = customerListPlansStatement.executeQuery();
		while(plan_set.next())
		{
			System.out.println("PlanID: " + plan_set.getInt(1));
			System.out.println("Name: " + plan_set.getString(2));
			System.out.println("Max Rentals: " + plan_set.getString(3));
			System.out.println("Monthly Price: $" + plan_set.getInt(4) + "\n");
		}	
	}

	public void transaction_rent(int cid, int mid) throws Exception {
	    /* rent the movie mid to the customer cid */
	    /* remember to enforce consistency ! */
		
		// check if it valid movie
		if(!isValidMovie(mid))
		{
			System.out.println("\n" + mid + " is not a valid movie.");
			return;
		}
		
		beginCustomerTransactionStatement.executeUpdate();
		lockCustomerTableStatement.executeUpdate();
		
		// check if movie is rented
		movieRentalStatement.clearParameters();
		movieRentalStatement.setInt(1,mid);
		ResultSet movierental_set = movieRentalStatement.executeQuery();
		int rCount = 0;
		while(movierental_set.next())
		{
			rCount++;	
			if(movierental_set.getInt(1) == cid)
			{
				System.out.println("You have ALREADY rented movie id " + mid);
			}
			else
			{
				System.out.println("Movie id: " + mid + " is UNAVAILABLE for rent.");
			}
		}
		
		if(rCount == 0)
		{
			// if available, rent the movie
			customerRentMovieStatement.clearParameters();
			customerRentMovieStatement.setInt(1,cid);
			customerRentMovieStatement.setInt(2,mid);
			customerRentMovieStatement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
			customerRentMovieStatement.executeUpdate();
		}
		
		int remainingRentals = getRemainingRentals(cid);
		// if this violates the terms of the plan, rollback
		if(remainingRentals < 0)
		{
			rollbackCustomerTransactionStatement.executeUpdate();
			System.out.println("You do not have enough movies in your plan to rent this movie. Request DENIED");
		}
		else
		{
			commitCustomerTransactionStatement.executeUpdate();
			System.out.println("You have rented movie id " + mid);
		}
	}

	public void transaction_return(int cid, int mid) throws Exception {

		beginCustomerTransactionStatement.executeUpdate();
		lockCustomerTableStatement.executeUpdate();
		
		// check if the user has actually rented the movie
		movieReturnStatement.clearParameters();
		movieReturnStatement.setInt(1, mid);
		movieReturnStatement.setInt(2, cid);
		
		int count = 0;
		ResultSet movie_rental = movieReturnStatement.executeQuery();
		if(movie_rental.next())
		{
			count++;
		}
		
		if(count == 0)
		{
			System.out.println("Movie id:" + mid + " is not checked out to you so you cannot return it.");
		}
		else
		{
			// return the movie
			customerReturnMovieStatement.clearParameters();
			customerReturnMovieStatement.setInt(1,cid);
			customerReturnMovieStatement.setInt(2,mid);
			customerReturnMovieStatement.executeUpdate();
		}
		
		commitCustomerTransactionStatement.executeUpdate();
	}

	public void transaction_fastSearch(int cid, String movie_title)
			throws Exception {
		/* like transaction_search, but uses joins instead of independent joins
		   Needs to run three SQL queries: (a) movies, (b) movies join directors (EC), (c) movies join actors
		   Answers are sorted by mid.
		   Then merge-joins the three answer sets */
		int mid = -1;
		List<String> listActors = new ArrayList<String>();
		List<String> listDirectors = new ArrayList<String>();
		String director = "";
		String actor = "";
		
		String searchString = "%" + movie_title + "%"; 
		fastSearchStatement.clearParameters();
		fastSearchStatement.setString(1,searchString);
		fastSearchStatement.setString(2,searchString);
		
		ResultSet resultSet = fastSearchStatement.executeQuery();
		while(resultSet.next())
		{
			if(mid != resultSet.getInt(1))
			{
				// print unique directors and actors from the previous movie
				PrintUniqueActorsAndDirectors(listActors, listDirectors);
				
				listActors = new ArrayList<String>();
				listDirectors = new ArrayList<String>();
				
				mid = resultSet.getInt(1);
			
				System.out.println("\nID: " + mid);
				System.out.println("  Name: " + resultSet.getString(2));
				
				int year = resultSet.getInt(3);
				if(year != 0) // 0 == null
				{
					System.out.println("  Year: " + year);
				}
			}
			
			if(resultSet.getString(6) != null && resultSet.getString(7) != null)
			{
				if(!director.equals(resultSet.getString(6) + " " + resultSet.getString(7)))
				{
					director = resultSet.getString(6) + " " + resultSet.getString(7);
					listDirectors.add(director);
				}
			}
			
			if(resultSet.getString(4) != null && resultSet.getString(5) != null)
			{
				if(!actor.equals(resultSet.getString(4) + " " + resultSet.getString(5)))
				{
					actor = resultSet.getString(4) + " " + resultSet.getString(5);
					listActors.add(actor);
				}
			}
		}
	}
}
