
import java.io.Serializable;
import java.util.Comparator;

public class TipAmountComparator implements Comparator<EventData>, Serializable {

	/**
	*
	*/
	private static final long serialVersionUID = 6864561826885961522L;

	@Override
	public int compare(EventData o1, EventData o2) {

		return Float.compare(o1.getTipAmount(), o2.getTipAmount());
	}

}
