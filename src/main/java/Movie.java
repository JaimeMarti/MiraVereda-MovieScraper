import java.sql.Date;

public class Movie {
    public String genre;
    public Date releaseDate;
    public int duration;
    public String title;
    public int price;
    public String description;
    public String langVersion;
    public String posterUrl;
    public String actors;
    public double vote;

    public Movie(String genre, Date releaseDate, int duration, String title, int price, String description, String langVersion, String posterUrl, String actors, double vote) {
        this.genre = genre;
        this.releaseDate = releaseDate;
        this.duration = duration;
        this.title = title;
        this.price = price;
        this.description = description;
        this.langVersion = langVersion;
        this.posterUrl = posterUrl;
        this.actors = actors;
        this.vote = vote;
    }
}
