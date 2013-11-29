import java.util.Comparator;


public class KVCompare implements Comparator<KeyValue>{

	public int compare(KeyValue first, KeyValue second) {
		if(first.getValue()<second.getValue())
			return 1;
		else if(first.getValue()>second.getValue())
			return -1;
		else {
			if(first.getKey()<second.getKey())
				return -1;
			else if(first.getKey()>second.getKey())
				return 1;
			else
				return 0;
		}
	}

}
