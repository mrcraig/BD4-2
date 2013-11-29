import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;


public class RTCompare implements Comparator<RevTime>{

	@Override
	public int compare(RevTime rt1, RevTime rt2) {
		Date t1 = new Date(rt1.getTs());
		Date t2 = new Date(rt2.getTs());
		
		if(t1.before(t2))
			return 1;
		else if(t1.after(t2))
			return -1;
		else
			return 0;
		
	}

}
