import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

	public static void main(String[] args) {
		lookForDashes("00001, 78888-78890, 99000, 70000-70010");
	}

	private static ArrayList<String> lookForDashes(String paragraph) {
		ArrayList<String> arr = new ArrayList<>();
		if (paragraph == null) {
			return arr;
		}
		Pattern p = Pattern.compile("\\d{5}-\\d{5}");
		Matcher m = p.matcher(paragraph);
		while (m.find()) {
			ArrayList<String> al = findValues(Integer.parseInt(m.group().substring(0, m.group().indexOf("-"))), Integer.parseInt(m.group().substring(m.group().indexOf("-")+1)));
			
			for(int i = 0; i < al.size(); i++) {
				arr.add(al.get(i));
			}
		}
		System.out.println("arr: "+  arr);
		//lookForDashes(Integer.parseInt(), Integer.parseInt());
		return arr;
	}
	
	private static ArrayList<String> findValues(int beg, int end){
		ArrayList <String> al = new ArrayList<>();
		for(int i = beg+1; i < end; i++) {
			al.add(i+"");
		}
		return al;
	}
}
