
import java.io.Serializable;
import java.util.Comparator;

public class DateComparator implements Comparator<EventData>, Serializable {

	/**
	*
	*/
	private static final long serialVersionUID = 819490992751238010L;

	@Override
	public int compare(EventData o1, EventData o2) {
		return o1.getPickupDateTime().compareTo(o2.getPickupDateTime());
	}

}
