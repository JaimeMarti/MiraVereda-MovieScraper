import com.google.gson.*;
import com.opencsv.CSVParser;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws FileNotFoundException, IOException, SQLException, ParseException {
        long nextActorId = 0;
        Map<String, Long> actorsMap = new HashMap<>();
        Map<Long, Set<Long>> movieActors = new HashMap<>();
        int processedMovieCount = 0;
        int skippedMovieCount = 0;
        CSVParser parser = new CSVParser();
        Gson gson = new Gson();
        DataSource dataSource = MyDataSource.getMyDataSource();
        Connection connection = dataSource.getConnection();
        System.out.println("Reading actors");
        BufferedReader actorReader = new BufferedReader(new FileReader("../../Downloads/credits.csv"));
        actorReader.readLine();
        while(true) {
            String line = actorReader.readLine();
            if(line == null)
                break;
            String[] fields = parser.parseLine(line);
            Set<Long> thisMovieActors = new HashSet<>();
            long movieId = Long.parseLong(fields[2]);
            try {
                JsonArray actors = gson.fromJson(fields[0], JsonArray.class);
                for(JsonElement actorElem: actors.asList()) {
                    JsonObject actor = actorElem.getAsJsonObject();
                    String name = actor.get("name").getAsString();
                    if(actorsMap.containsKey(name)) {
                        thisMovieActors.add(actorsMap.get(name));
                    }
                    else {
                        actorsMap.put(name, nextActorId);
                        thisMovieActors.add(nextActorId);
                        nextActorId++;
                    }
                }
                movieActors.put(movieId, thisMovieActors);
            }
            catch(JsonSyntaxException e) {
                skippedMovieCount++;
            }
        }
        System.out.println("Read all actors");
        String sql = "INSERT INTO ACTOR VALUES (?, ?, ?, (SELECT CURRENT_TIMESTAMP FROM DUAL))";
        PreparedStatement st = null;
        int actorIter = 0;
        for(Map.Entry<String, Long> entry: actorsMap.entrySet()) {
            if(actorIter % 500 == 0) {
                if(st != null) {
                    st.executeBatch();
                    System.out.println("Inserted " + actorIter + " actors");
                    st.close();
                }
                st = connection.prepareCall(sql);
            }
            Long id = entry.getValue();
            String fullName = entry.getKey();
            String name;
            String surname;
            if(fullName.contains(" ")) {
                name = fullName.split(" ")[0];
                surname = fullName.substring(name.length() + 1);
                while(surname.getBytes().length > 30)
                    surname = surname.substring(0, surname.length() - 1);
            }
            else {
                name = fullName;
                surname = " ";
            }
            if(name.isEmpty())
                name = " ";
            if(surname.isEmpty())
                surname = " ";
            st.setString(1, Long.toString(id));
            st.setString(2, name);
            st.setString(3, surname);
            st.addBatch();
            actorIter++;
        }
        st.executeBatch();
        st.close();
        SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd");
        List<Movie> movies = new ArrayList<>();
        System.out.println("Reading movies");
        BufferedReader movieReader = new BufferedReader(new FileReader("../../Downloads/movies_metadata.csv"));
        movieReader.readLine();
        while(true) {
            String line = movieReader.readLine();
            if(line == null)
                break;
            try {
                String[] fields = parser.parseLine(line);
                if(fields.length < 21) {
                    skippedMovieCount++;
                    continue;
                }
                long movieId = Long.parseLong(fields[5]);
                if(!movieActors.containsKey(movieId))
                    continue;
                if(fields[16].isEmpty() || fields[14].isEmpty()) {
                    skippedMovieCount++;
                    continue;
                }
                String title = fields[20];
                while(title.getBytes().length > 30)
                    title = title.substring(0, title.length() - 1);
                String description = fields[9];
                if(description.isEmpty()) {
                    skippedMovieCount++;
                    continue;
                }
                while(description.getBytes().length > 300)
                    description = description.substring(0, description.length() - 10);
                int duration = (int)Double.parseDouble(fields[16]) * 60;
                Date releaseDate = new Date(dateParser.parse(fields[14]).getTime());
                String genres = fields[3];
                String collection = fields[1];
                double vote = Double.parseDouble(fields[22]);
                String isAdult = fields[0];
                String langVersion = Math.random() > 0.5 ? "V.O.S.E." : "V.E.";
                int price = (int)(1d + Math.random() * 3d * 100d);
                if(isAdult.equals("True") || collection.isEmpty())
                    continue;
                JsonArray genresArray = gson.fromJson(genres, JsonArray.class);
                if(genresArray.isEmpty()) {
                    skippedMovieCount++;
                    continue;
                }
                String genre = genresArray.get(0).getAsJsonObject().get("name").getAsString();
                JsonObject collectionObj = gson.fromJson(collection, JsonObject.class);
                String posterName = collectionObj.get("poster_path").getAsString();
                if(posterName.equals("None")) {
                    skippedMovieCount++;
                    continue;
                }
                String posterUrl = "https://image.tmdb.org/t/p/w500" + posterName;
                String actors = movieActors.get(movieId).stream().map(String::valueOf).collect(Collectors.joining(","));
                if(!actors.isEmpty())
                    actors += ",";
                movies.add(new Movie(genre, releaseDate, duration, title, price, description, langVersion, posterUrl, actors, vote));
            }
            catch(IOException e) {
                skippedMovieCount++;
            }
        }
        System.out.println("Read all movies");
        movies.sort((a, b) -> Double.compare(a.vote, b.vote));
        Date aFewDaysFromNow = new Date(new java.util.Date().getTime() + 2L * 30L * 24L * 60L * 60L * 1000L);
        CallableStatement cst = null;
        sql = "{ call crear_pelicula_sin_out(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) }";
        int movieIter = 0;
        for(Movie movie: movies) {
            if(movieIter % 100 == 0) {
                if(cst != null) {
                    cst.executeBatch();
                    System.out.println("Inserted " + movieIter + " movies");
                    cst.close();
                }
                cst = connection.prepareCall(sql);
            }
            cst.setString(1, "pelicula");
            cst.setString(2, movie.genre);
            cst.setDate(3, movie.releaseDate);
            cst.setInt(4, movie.duration);
            cst.setString(5, movie.title);
            cst.setInt(6, movie.price);
            cst.setString(7, movie.description);
            cst.setString(8, "Jaime Martí");
            cst.setString(9, movie.langVersion);
            cst.setInt(10, 1);
            cst.setString(11, movie.posterUrl);
            cst.setDate(12, aFewDaysFromNow);
            cst.setNull(13, Types.DATE);
            cst.setNull(14, Types.INTEGER);
            cst.setNull(15, Types.INTEGER);
            cst.setString(16, movie.actors);
            cst.addBatch();
            processedMovieCount++;
            movieIter++;
        }
        cst.executeBatch();
        cst.close();
        System.out.println("Total processed movies: " + processedMovieCount);
        System.out.println("Total skipped movies: " + skippedMovieCount);
    }
}
