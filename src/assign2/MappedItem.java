package assign2;

/**
 * Created by cianduffy on 18/03/2017.
 */
public class MappedItem {
    private final String letter;
    private final String file;

    MappedItem(String entry, String file) {
        this.letter = entry;
        this.file = file;
    }

    String getFirstLetter() {
        return letter;
    }

    String getFile() {
        return file;
    }

    @Override
    public String toString() {
        return "[\"" + letter + "\",\"" + file + "\"]";
    }
}
